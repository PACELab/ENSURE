/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorSystem
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.collection.mutable //avs
import scala.collection.mutable.ListBuffer
//import scala.collection.immutable //avs


/**
 * Abstract class which provides common logic for all LoadBalancer implementations.
 */
abstract class CommonLoadBalancer(config: WhiskConfig,
                                  feedFactory: FeedFactory,
                                  loadFeedFactory: FeedFactory, // avs
                                  controllerInstance: ControllerInstanceId)(implicit val actorSystem: ActorSystem,
                                                                            logging: Logging,
                                                                            materializer: ActorMaterializer,
                                                                            messagingProvider: MessagingProvider)
    extends LoadBalancer {

  protected implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  val adaptiveLbConfig: AdaptiveContainerPoolBalancerConfig =
    loadConfigOrThrow[AdaptiveContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  val rrLbConfig: RoundRobinContainerPoolBalancerConfig =
    loadConfigOrThrow[RoundRobinContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  val leastConn_lbConfig: LeastConnectionsContainerPoolBalancerConfig =
    loadConfigOrThrow[LeastConnectionsContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  protected val invokerPool: ActorRef

  // avs --begin
  protected[loadBalancer] var allTrackedActions = mutable.Map.empty[String, ActionStats] 
  protected[loadBalancer] var allRunningActions = mutable.Map.empty[String, Int] 
  var allInvokers = mutable.Map.empty[InvokerInstanceId, AdapativeInvokerStats]

  var lastUpdatedTime: Long = 0
  var scanIntervalInMS: Long = 60*1000 // 60 seconds!
  // i.e. if  I have more than (acceptableUnsafeInvokerRatio*100)% invokers which are unsafe in either of the workload types, I will upgrade an invoker..


  var acceptableUnsafeInvokerRatio = 0.75 
  val numProactiveContsToSpawn = 2
  val curInvokerProactiveContsToSpawn = 1

  // avs --end
  /** State related to invocations and throttling */
  protected[loadBalancer] val activationSlots = TrieMap[ActivationId, ActivationEntry]()
  protected[loadBalancer] val activationPromises =
    TrieMap[ActivationId, Promise[Either[ActivationId, WhiskActivation]]]()
  protected val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  protected val totalActivations = new LongAdder()
  protected val totalBlackBoxActivationMemory = new LongAdder()
  protected val totalManagedActivationMemory = new LongAdder()

  protected def emitMetrics() = {
    MetricEmitter.emitGaugeMetric(LOADBALANCER_ACTIVATIONS_INFLIGHT(controllerInstance), totalActivations.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, ""),
      totalBlackBoxActivationMemory.longValue + totalManagedActivationMemory.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, "Blackbox"),
      totalBlackBoxActivationMemory.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, "Managed"),
      totalManagedActivationMemory.longValue)
  }

  actorSystem.scheduler.schedule(10.seconds, 10.seconds)(emitMetrics())

  override def activeActivationsFor(namespace: UUID): Future[Int] =
    Future.successful(activationsPerNamespace.get(namespace).map(_.intValue()).getOrElse(0))
  override def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue())

  /**
   * 2. Update local state with the activation to be executed scheduled.
   *
   * All activations are tracked in the activationSlots map. Additionally, blocking invokes
   * are tracked in the activation results map. When a result is received via activeack, it
   * will cause the result to be forwarded to the caller waiting on the result, and cancel
   * the DB poll which is also trying to do the same.
   */
  protected def setupActivation(msg: ActivationMessage,
                                action: ExecutableWhiskActionMetaData,
                                instance: InvokerInstanceId): Future[Either[ActivationId, WhiskActivation]] = {

    totalActivations.increment()
    val isBlackboxInvocation = action.exec.pull
    val totalActivationMemory =
      if (isBlackboxInvocation) totalBlackBoxActivationMemory else totalManagedActivationMemory
    totalActivationMemory.add(action.limits.memory.megabytes)

    activationsPerNamespace.getOrElseUpdate(msg.user.namespace.uuid, new LongAdder()).increment()

    // Timeout is a multiple of the configured maximum action duration. The minimum timeout is the configured standard
    // value for action durations to avoid too tight timeouts.
    // Timeouts in general are diluted by a configurable factor. In essence this factor controls how much slack you want
    // to allow in your topics before you start reporting failed activations.
    val timeout = (action.limits.timeout.duration.max(TimeLimit.STD_DURATION) * lbConfig.timeoutFactor) + 1.minute

    val resultPromise = if (msg.blocking) {
      activationPromises.getOrElseUpdate(msg.activationId, Promise[Either[ActivationId, WhiskActivation]]()).future
    } else Future.successful(Left(msg.activationId))

    // Install a timeout handler for the catastrophic case where an active ack is not received at all
    // (because say an invoker is down completely, or the connection to the message bus is disrupted) or when
    // the active ack is significantly delayed (possibly dues to long queues but the subject should not be penalized);
    // in this case, if the activation handler is still registered, remove it and update the books.
    activationSlots.getOrElseUpdate(
      msg.activationId, {
        val timeoutHandler = actorSystem.scheduler.scheduleOnce(timeout) {
          processCompletion(msg.activationId, msg.transid, forced = true, isSystemError = false, invoker = instance)
        }

        // please note: timeoutHandler.cancel must be called on all non-timeout paths, e.g. Success
        ActivationEntry(
          msg.activationId,
          msg.user.namespace.uuid,
          instance,
          action.limits.memory.megabytes.MB,
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          timeoutHandler,
          isBlackboxInvocation)
      })

    resultPromise
  }

  protected val messageProducer =
    messagingProvider.getProducer(config, Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT))

  /** 3. Send the activation to the invoker */
  protected def sendActivationToInvoker(producer: MessageProducer,
                                        msg: ActivationMessage,
                                        invoker: InvokerInstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val topic = s"invoker${invoker.toInt}"

    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${msg.activationId}'",
      logLevel = InfoLevel)

    producer.send(topic, msg).andThen {
      case Success(status) =>
        transid.finished(
          this,
          start,
          s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
          logLevel = InfoLevel)
      case Failure(_) => transid.failed(this, start, s"error on posting to topic $topic")
    }

    // avs --begin
    /*val a_topic = s"load-invoker${invoker.toInt}"
    val a_msg: LoadMessage = LoadMessage(s"SanityCheck${invoker.toInt} and activID : ${msg.activationId}")
    producer.send(a_topic, a_msg).andThen {
      case Success(status) =>
        logging.info(this, s"<adbg> On topic: ${a_topic} SUCCESSFULLY posted message: ${a_msg} ")
      case Failure(_) => 
        logging.info(this, s"<adbg> On topic: ${a_topic} DIDNOT post message: ${a_msg} ")
    } */   
    // avs --end
  }

  /**
   * Subscribes to active acks (completion messages from the invokers), and
   * registers a handler for received active acks from invokers.
   */
  private val activationFeed: ActorRef =
    feedFactory.createFeed(actorSystem, messagingProvider, processAcknowledgement)

  /** 4. Get the active-ack message and parse it */
  protected[loadBalancer] def processAcknowledgement(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    AcknowledegmentMessage.parse(raw) match {
      case Success(m: CompletionMessage) =>
        processCompletion(
          m.activationId,
          m.transid,
          forced = false,
          isSystemError = m.isSystemError,
          invoker = m.invoker)
        activationFeed ! MessageFeed.Processed

      case Success(m: ResultMessage) =>
        processResult(m.response, m.transid)
        activationFeed ! MessageFeed.Processed

      case Failure(t) =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw")

      case _ =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"Unexpected Acknowledgment message received by loadbalancer: $raw")
    }
  }

// avs --begin

  type ntuiType = (ListBuffer[InvokerHealth]) => Boolean
  //def needToUpgradeInvoker(activeInvokers: ListBuffer[InvokerHealth]): Boolean = {
  val nTUI: ntuiType = (activeInvokers: ListBuffer[InvokerHealth]) => {
        var numUnsafeInvokers = 0 
        var retVal: Boolean = false

        activeInvokers.foreach{
          curInvoker =>
            var someWorkloadUnsafe: Boolean = false
            allInvokers.get(curInvoker.id) match {
              case Some(curInvokerStats) =>
                logging.info(this,s"<adbg> in <nTUI> invoker: ${curInvoker.id.toInt} is PRESENT in allInvokers. Before checking this invoker, numUnsafeInvokers: ${numUnsafeInvokers} ")
                someWorkloadUnsafe = curInvokerStats.isInvokerUnsafe()
              case None =>
                allInvokers = allInvokers + (curInvoker.id -> new AdapativeInvokerStats(curInvoker.id,curInvoker.status,logging) )
                logging.info(this,s"<adbg> in <nTUI> invoker: ${curInvoker.id.toInt} is ABSENT in allInvokers. Before checking this invoker, numUnsafeInvokers: ${numUnsafeInvokers}. This shouldn't happen, HANDLE it!!")
                var tempInvokerStats = allInvokers(curInvoker.id)
                tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
                someWorkloadUnsafe = tempInvokerStats.isInvokerUnsafe()
              }          
            if(someWorkloadUnsafe)
              numUnsafeInvokers+=1
        }

        if(activeInvokers.size > 0){
          if( (numUnsafeInvokers.toDouble/activeInvokers.size) > acceptableUnsafeInvokerRatio ){
            logging.info(this,s"<adbg> in <nTUI> <Res-1> numUnsafeInvokers: ${numUnsafeInvokers} activeInvokers.size: ${activeInvokers.size} and acceptableUnsafeInvokerRatio: ${acceptableUnsafeInvokerRatio}")
            retVal = true
          }else{
            logging.info(this,s"<adbg> in <nTUI> <Res-2> numUnsafeInvokers: ${numUnsafeInvokers} activeInvokers.size: ${activeInvokers.size} and acceptableUnsafeInvokerRatio: ${acceptableUnsafeInvokerRatio}")
          }
        }

      retVal 
    }

    def perActInvkrCheck(toRemoveBuf:ListBuffer[InvokerInstanceId]): Unit ={

      allRunningActions.keys.foreach{
        curActName =>
        var numInvkrs = 0
        allRunningActions.get(curActName) match {
          case Some(curActNumInvkrs) => 
            logging.info(this,s"<adbg> <pAIC:1> action: ${curActName} is PRESENT in allRunningActions")
            var curActStats = allTrackedActions(curActName) // since everything is prepopulated at init, this should be OK! 
            //numInvkrs = curActStats.checkExpiredInvkrs(toRemoveBuf)
            numInvkrs = curActStats.checkRunningInvkrStatus() //toRemoveBuf)
          case None => 
            logging.info(this,s"<adbg> <pAIC:2> action: ${curActName} is ABSENT in allRunningActions")
        }
        if(numInvkrs==0)
          logging.info(this,s"<adbg> <pAIC:3> action: ${curActName} has zero invokers, should remove it from allRunningActions")                
        else
          logging.info(this,s"<adbg> <pAIC:4> action: ${curActName} has ${numInvkrs} invokers, should retain it from allRunningActions")                
        allRunningActions(curActName) = numInvkrs
      }
    }

    def needToDowngradeInvoker(activeInvokers: ListBuffer[InvokerHealth]): ListBuffer[InvokerHealth] = {
      var toRemoveBuf: ListBuffer[InvokerInstanceId] = new mutable.ListBuffer[InvokerInstanceId]
      var dummyBuf: ListBuffer[InvokerHealth] = new mutable.ListBuffer[InvokerHealth]

      allInvokers.keys.foreach{
        curInvoker =>
        var curInvokerStats = allInvokers(curInvoker)
        var numConts = curInvokerStats.getActiveNumConts()
        if(numConts == 0){ // don't have any active containers..
          toRemoveBuf+=curInvoker
        }
        logging.info(this,s"<adbg> in <nTDI> invoker: ${curInvoker.toInt} has ${numConts} containers. toRemoveBuf.size: ${toRemoveBuf.size} ")
      }
      //if(toRemoveBuf.size>0)
      perActInvkrCheck(toRemoveBuf)
      dummyBuf
    }

  //getNumConnections: (InvokerHealth,String) => Int, // needToUpgradeInvoker: (ListBuffer[InvokerHealth])=> Boolean 
  type gncType = (InvokerInstanceId,String) => Int    
  val gNC: gncType = (invoker: InvokerInstanceId, actionName: String) => {
    logging.info(this,s"<adbg> <getNumConnections> on invoker: ${invoker.toInt}")
    allInvokers.get(invoker) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<adbg> in <getNumConnections> invoker: ${invoker.toInt} is PRESENT in allInvokers ")
        curInvokerStats.getNumConnections(actionName)
      case None =>
        allInvokers = allInvokers + (invoker -> new AdapativeInvokerStats(invoker,InvokerState.Healthy,logging) )
        logging.info(this,s"<adbg> in <getNumConnections> invoker: ${invoker.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats.getNumConnections(actionName)
      }     
  }

  //issuedAReq: (InvokerHealth,String) => Unit,
  type iarType = (InvokerInstanceId,String) => Unit    
  val iAR: iarType = (invoker: InvokerInstanceId, actionName: String) => {
    logging.info(this,s"<adbg> <iAR> on invoker: ${invoker.toInt}")
    allInvokers.get(invoker) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<adbg> in <iAR> invoker: ${invoker.toInt} is PRESENT in allInvokers ")
        curInvokerStats.issuedAReq(actionName,1)
      case None =>
        allInvokers = allInvokers + (invoker -> new AdapativeInvokerStats(invoker,InvokerState.Healthy,logging) )
        logging.info(this,s"<adbg> in <iAR> invoker: ${invoker.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats.issuedAReq(actionName,1)
      }     
  }  

  //type gifaType (String,InvokerHealth) => Option[InvokerInstanceId]
  type guifaType = (String) => Option[(InvokerInstanceId,allocMetadata)]//Option[InvokerInstanceId]
  //def getUsedInvokerForAction(actionName: String,invoker: InvokerHealth): Option[InvokerInstanceId] = {
  val gUIFA: guifaType = (actionName: String) => {
    allTrackedActions.get(actionName) match {
      case Some(curActStats) => 
        logging.info(this,s"<adbg> <gUIFA> action: ${actionName} is PRESENT in allTrackedActions")
        //curActStats.getUsedInvoker()
        allRunningActions.get(actionName) match{
          case Some(curActStats) => 
            logging.info(this,s"<adbg> <gUIFA> action: ${actionName} is PRESENT in allRunningActions, nothing doing..")

          case None => 
            logging.info(this,s"<adbg> <gUIFA> action: ${actionName} is ABSENT in allRunningActions, adding it..")
            allRunningActions = allRunningActions + (actionName-> 1)
        }

        curActStats.getAutoScaleUsedInvoker(numProactiveContsToSpawn,curInvokerProactiveContsToSpawn)
      case None => 
        logging.info(this,s"<adbg> <gUIFA> action: ${actionName} is ABSENT in allTrackedActions")
        
        allTrackedActions = allTrackedActions + (actionName-> new ActionStats(actionName,logging))
        var myActStats :ActionStats = allTrackedActions(actionName)
        //myActStats.getUsedInvoker()
        myActStats.getAutoScaleUsedInvoker(numProactiveContsToSpawn,curInvokerProactiveContsToSpawn)
    }
  } 
  
  //def getActiveInvoker(actionName:String,curActiveInvoker:InvokerHealth,activeInvokers:IndexedSeq[InvokerHealth]): Option[InvokerInstanceId]
  type gaiType =  (String,ListBuffer[InvokerHealth]) => Option[(InvokerInstanceId,allocMetadata)] //Option[InvokerInstanceId]
  val gaI: gaiType = (actionName:String,activeInvokers:ListBuffer[InvokerHealth]) => {
    allTrackedActions.get(actionName) match {
      case Some(curActStats) => 
        logging.info(this,s"<adbg> <gAI-0> action: ${actionName} is PRESENT in allTrackedActions")
        //curActStats.getActiveInvoker(activeInvokers)
        curActStats.getActiveInvoker(0,numProactiveContsToSpawn,curInvokerProactiveContsToSpawn)
      case None => 
        logging.info(this,s"<adbg> <gAI-0> action: ${actionName} is ABSENT in allTrackedActions")
        allTrackedActions = allTrackedActions + (actionName-> new ActionStats(actionName,logging))
        var myActStats :ActionStats = allTrackedActions(actionName)
        //myActStats.getActiveInvoker(activeInvokers)
        myActStats.getActiveInvoker(0,numProactiveContsToSpawn,curInvokerProactiveContsToSpawn)
    }
  }

  type aitype = (InvokerHealth,Int,Int) => Unit
  val aIT: aitype = (invoker: InvokerHealth,numCores:Int,memorySize:Int) => {
  //def addInvokerTracking(invoker: InvokerInstanceId,numCores:Int,memorySize:Int): Unit = {
    logging.info(this,s"<adbg> <aIT> on invoker: ${invoker.id.toInt}")

    allInvokers.get(invoker.id) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<adbg> in <aIT> invoker: ${invoker.id.toInt} is PRESENT in allInvokers ")
      case None =>
        allInvokers = allInvokers + (invoker.id -> new AdapativeInvokerStats(invoker.id,invoker.status,logging) )
        logging.info(this,s"<adbg> in <aIT> invoker: ${invoker.id.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker.id)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
      } 

// add all the actions we want to track. 
// shoddy implementation, but have to do for now.
    var tempInvokerStats = allInvokers(invoker.id)
    if(invoker.id.toInt>=0){ 
      var actionsToAdd: ListBuffer[String] = new mutable.ListBuffer[String]
      actionsToAdd+= "imageResizing_v1"; actionsToAdd+= "rodinia_nn_v1"; actionsToAdd+= "euler3d_cpu_v1"; actionsToAdd+= "servingCNN_v1"
      actionsToAdd+= "realTimeAnalytics_v1"; actionsToAdd+= "invokerHealthTestAction0"

      actionsToAdd+= "matmulaction_v1";actionsToAdd+= "matmulaction_v2";actionsToAdd+= "matadd_v1";actionsToAdd+= "sort_v1"; actionsToAdd+= "chaos_v1";
      
      /*
      actionsToAdd+= "markdown2html"; actionsToAdd+= "sentiment"; actionsToAdd+= "ocr-img"; actionsToAdd+= "base64-python"; actionsToAdd+= "primes-python"; actionsToAdd+= "http-endpoint-python";
      actionsToAdd+= "go_v1"; actionsToAdd+= "2to3_v1"; actionsToAdd+= "chameleon_v1"; actionsToAdd+= "crypto_pyaes_v1"; 
      actionsToAdd+= "deltablue_v1"; actionsToAdd+= "django_template_v1"; actionsToAdd+= "dulwich_log_v1"; actionsToAdd+= "fannkuch_v1"; actionsToAdd+= "float_v1"; actionsToAdd+= "genshi_text_v1";

      actionsToAdd+= "genshi_xml_v1"; actionsToAdd+= "hexiom_v1"; actionsToAdd+= "html5lib_v1"; actionsToAdd+= "json_dumps_v1";
      actionsToAdd+= "mako_v1"; actionsToAdd+= "meteor_contest_v1"; actionsToAdd+= "nbody_v1"; actionsToAdd+= "nqueens_v1";
      actionsToAdd+= "pathlib_v1"; actionsToAdd+= "pidigits_v1"; actionsToAdd+= "ray_trace";

      actionsToAdd+= "regex_compile_v1"; actionsToAdd+= "regex_dna_v1"; actionsToAdd+= "richards_v1"; actionsToAdd+= "scimark_fft_v1";
      actionsToAdd+= "scimark_lu"; actionsToAdd+= "spectral_norm_v1"; actionsToAdd+= "sqlalchemy_declarative_v1"; actionsToAdd+= "sympy_expand_v1"; actionsToAdd+= "sympy_integrate_v1";

      actionsToAdd+= "sympy_v1"; actionsToAdd+= "sympy_sum_v1"; actionsToAdd+= "sympy_str_v1"; actionsToAdd+= "tornado_http_v1"; actionsToAdd+= "xml_etree_parse_v1";
      actionsToAdd+= "xml_etree_iterparse_v1"; actionsToAdd+= "xml_etree_generate_v1"; actionsToAdd+= "xml_etree_process_v1";*/
      
      actionsToAdd+= "emailGen_v1"; actionsToAdd+= "stockAnalysis_v1"; actionsToAdd+= "fileEncrypt_v1"; actionsToAdd+= "serving_lrReview_v1"; 
      actionsToAdd+= "fe_v1"; actionsToAdd+= "fe_v2"; actionsToAdd+= "fe_v3"; actionsToAdd+= "fe_v4";

      actionsToAdd.foreach {
        curAction =>
          allTrackedActions.get(curAction) match {
            case Some(curActStats) => 
              logging.info(this,s"<adbg> <aIT> action: ${curAction} is PRESENT in allTrackedActions and it is being initialized for invoker: ${invoker.id.toInt}")
              //curActStats.addActionStats(m.invoker,m.latency,m.numConts) // curActStats.simplePrint(m.curActName,m.latency,m.numConts,logging)
              curActStats.initAllInvokers(invoker.id,tempInvokerStats,0,10,0) // hack for new hybrid multinode algo! 
              if(invoker.id.toInt==0) curActStats.addActionStats(invoker.id,tempInvokerStats,0,10,0,0)

            case None => 
              logging.info(this,s"<adbg> <aIT> action: ${curAction} is ABSENT in allTrackedActions  and it ran on invoker: ${invoker.id.toInt}")              
              allTrackedActions = allTrackedActions + (curAction -> new ActionStats(curAction,logging))
              //allTrackedActions = allTrackedActions + (m.curActName -> curInvokerStats)
              var myActStats :ActionStats = allTrackedActions(curAction)
              myActStats.initAllInvokers(invoker.id,tempInvokerStats,0,10,0) // hack for new hybrid multinode algo! 
              if(invoker.id.toInt==0) myActStats.addActionStats(invoker.id,tempInvokerStats,0,10,0,0)

        }
      }
    }
  }  
   
  def getInvokerTracking(invoker: InvokerInstanceId): AdapativeInvokerStats = {
    logging.info(this,s"<adbg> <gIT> on invoker: ${invoker.toInt}")
    allInvokers.get(invoker) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<adbg> in <gIT> invoker: ${invoker.toInt} is PRESENT in allInvokers ")
        curInvokerStats
      case None =>
        allInvokers = allInvokers + (invoker -> new AdapativeInvokerStats(invoker,InvokerState.Healthy,logging) )
        logging.info(this,s"<adbg> in <gIT> invoker: ${invoker.toInt} is ABSENT in allInvokers. Didn't expect this, HANDLE it!! ")
        var tempInvokerStats = allInvokers(invoker)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats
    } 
  }

  /*def issuedSomeDummyReqs(invoker: InvokerInstanceId,actionName:String,numContsSpawnedInMyInvoker: Int): Unit = {
    logging.info(this,s"<adbg> <ISDR> on invoker: ${invoker.toInt}")
    allInvokers.get(invoker) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<adbg> in <ISDR> invoker: ${invoker.toInt} is PRESENT in allInvokers ")
        curInvokerStats.issuedSomeDummyReqs(actionName,numContsSpawnedInMyInvoker)
      case None =>
        allInvokers = allInvokers + (invoker -> new AdapativeInvokerStats(invoker,InvokerState.Healthy,logging) )
        logging.info(this,s"<adbg> in <ISDR> invoker: ${invoker.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats.issuedSomeDummyReqs(actionName,numContsSpawnedInMyInvoker)
      }    
  }*/

  type cicType = (InvokerHealth,String) => Boolean
  val cIC: cicType = (invoker: InvokerHealth,actionName:String) => {
    //def checkInvokerCapacity(invoker: InvokerInstanceId): Boolean = {
    logging.info(this,s"<adbg> <cIC> on invoker: ${invoker.id.toInt}")
    allInvokers.get(invoker.id) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<adbg> in <cIC> invoker: ${invoker.id.toInt} is PRESENT in allInvokers ")
        val (numInFlightReqs,curAvgLatency,curActOpZone,decision) = curInvokerStats.capacityRemaining(actionName)
        decision
      case None =>
        allInvokers = allInvokers + (invoker.id -> new AdapativeInvokerStats(invoker.id,invoker.status,logging) )
        logging.info(this,s"<adbg> in <cIC> invoker: ${invoker.id.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker.id)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        val (numInFlightReqs,curAvgLatency,curActOpZone,decision) = tempInvokerStats.capacityRemaining(actionName)
        decision
      } 
  }

  def addActionStatsToInvoker(invoker: InvokerInstanceId,toUpdateAction:String,latencyVal: Long, initTime: Long, toUpdateNumConts:Int,toUpdateNumWindowConts: Int): Unit = {
    logging.info(this,s"<adbg> <aASTI> on invoker: ${invoker.toInt}")
    allInvokers.get(invoker) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<adbg> in <aASTI> invoker: ${invoker.toInt} is PRESENT in allInvokers ")
        curInvokerStats.updateActionStats(toUpdateAction,latencyVal,initTime,toUpdateNumConts,toUpdateNumWindowConts)
      case None =>
        allInvokers = allInvokers + (invoker -> new AdapativeInvokerStats(invoker,InvokerState.Healthy,logging) )
        logging.info(this,s"<adbg> in <aASTI> invoker: ${invoker.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats.updateActionStats(toUpdateAction,latencyVal,initTime,toUpdateNumConts,toUpdateNumWindowConts)
      } 
  } 

  private val loadRespFeed: ActorRef =
    loadFeedFactory.createFeed(actorSystem, messagingProvider, processLoadResponse)

  protected[loadBalancer] def processLoadResponse(bytes: Array[Byte]): Future[Unit] = Future{
    logging.info(this, s"<adbg> <pLR> 1")
    val raw = new String(bytes, StandardCharsets.UTF_8)

    ActionStatsMessage.parse(raw) match {
      
      case Success(m: ActionStatsMessage) =>
        logging.info(this,s"<adbg> <pLR> ActionStatsMessage successfully parsed! m.Str: ${m.toString}")
        var curInvokerStats = getInvokerTracking(m.invoker) // assuming that this'd never fail! 
        allTrackedActions.get(m.curActName) match {
          case Some(curActStats) => 
            logging.info(this,s"<adbg> <pLR> action: ${m.curActName} is PRESENT in allTrackedActions and it ran on invoker: ${m.invoker.toInt}")
            //curActStats.addActionStats(m.invoker,m.latency,m.numConts) // curActStats.simplePrint(m.curActName,m.latency,m.numConts,logging)
            curActStats.addActionStats(m.invoker,curInvokerStats,m.latency,m.initTime,m.numConts,m.numWindowConts)
          case None => 
            logging.info(this,s"<adbg> <pLR> action: ${m.curActName} is ABSENT in allTrackedActions  and it ran on invoker: ${m.invoker.toInt}")
            
            allTrackedActions = allTrackedActions + (m.curActName -> new ActionStats(m.curActName,logging))
            //allTrackedActions = allTrackedActions + (m.curActName -> curInvokerStats)
            var myActStats :ActionStats = allTrackedActions(m.curActName)
            myActStats.addActionStats(m.invoker,curInvokerStats,m.latency,m.initTime,m.numConts,m.numWindowConts) //myActStats.simplePrint(m.curActName,m.latency,m.numConts,logging)
        }

        //addActionStatsToInvoker(m.invoker,m.curActName,m.avgLatency,m.numConts)
      case Failure(t) =>
        logging.info(this,s"<adbg> <pLR> ActionStatsMessage wasn't parsed! raw: ${raw}")
    }    
    //logging.info(this, s"<adbg> <pLR> raw_message: ${raw} actStatsMsg --> {msg.toString} loadMsg --> {msg2.toString}")
    loadRespFeed ! MessageFeed.Processed
  }  
// avs --end

  /** 5. Process the result ack and return it to the user */
  protected def processResult(response: Either[ActivationId, WhiskActivation], tid: TransactionId): Unit = {
    val aid = response.fold(l => l, r => r.activationId)

    // Resolve the promise to send the result back to the user.
    // The activation will be removed from the activation slots later, when the completion message
    // is received (because the slot in the invoker is not yet free for new activations).
    activationPromises.remove(aid).foreach(_.trySuccess(response))
    logging.info(this, s"received result ack for '$aid'")(tid)
  }

  protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry)

  /** 6. Process the completion ack and update the state */
  protected[loadBalancer] def processCompletion(aid: ActivationId,
                                                tid: TransactionId,
                                                forced: Boolean,
                                                isSystemError: Boolean,
                                                invoker: InvokerInstanceId): Unit = {

    val invocationResult = if (forced) {
      InvocationFinishedResult.Timeout
    } else {
      // If the response contains a system error, report that, otherwise report Success
      // Left generally is considered a Success, since that could be a message not fitting into Kafka
      if (isSystemError) {
        InvocationFinishedResult.SystemError
      } else {
        InvocationFinishedResult.Success
      }
    }

    activationSlots.remove(aid) match {
      case Some(entry) =>
        totalActivations.decrement()
        val totalActivationMemory =
          if (entry.isBlackbox) totalBlackBoxActivationMemory else totalManagedActivationMemory
        totalActivationMemory.add(entry.memory.toMB * (-1))
        activationsPerNamespace.get(entry.namespaceId).foreach(_.decrement())

        releaseInvoker(invoker, entry)

        if (!forced) {
          entry.timeoutHandler.cancel()
          // notice here that the activationPromises is not touched, because the expectation is that
          // the active ack is received as expected, and processing that message removed the promise
          // from the corresponding map
        } else {
          // the entry has timed out; if the active ack is still around, remove its entry also
          // and complete the promise with a failure if necessary
          activationPromises
            .remove(aid)
            .foreach(_.tryFailure(new Throwable("no completion or active ack received yet")))
        }

        logging.info(this, s"${if (!forced) "received" else "forced"} completion ack for '$aid'")(tid)
        // Active acks that are received here are strictly from user actions - health actions are not part of
        // the load balancer's activation map. Inform the invoker pool supervisor of the user action completion.
        // guard this
        invokerPool ! InvocationFinishedMessage(invoker, invocationResult)
      case None if tid == TransactionId.invokerHealth =>
        // Health actions do not have an ActivationEntry as they are written on the message bus directly. Their result
        // is important to pass to the invokerPool because they are used to determine if the invoker can be considered
        // healthy again.
        logging.info(this, s"received completion ack for health action on $invoker")(tid)
        // guard this
        invokerPool ! InvocationFinishedMessage(invoker, invocationResult)
      case None if !forced =>
        // Received an active-ack that has already been taken out of the state because of a timeout (forced active-ack).
        // The result is ignored because a timeout has already been reported to the invokerPool per the force.
        logging.debug(this, s"received completion ack for '$aid' which has no entry")(tid)
      case None =>
        // The entry has already been removed by an active ack. This part of the code is reached by the timeout and can
        // happen if active-ack and timeout happen roughly at the same time (the timeout was triggered before the active
        // ack canceled the timer). As the active ack is already processed we don't have to do anything here.
        logging.debug(this, s"forced completion ack for '$aid' which has no entry")(tid)
    }
  }
}
