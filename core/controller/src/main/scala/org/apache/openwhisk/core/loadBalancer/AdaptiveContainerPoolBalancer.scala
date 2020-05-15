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
// avs --begin
package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import akka.actor.ActorRefFactory
//import java.util.concurrent.ThreadLocalRandom

//import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.core.loadBalancer.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader

import scala.annotation.tailrec
import scala.concurrent.Future

// avs --begin
import scala.collection.mutable 
import scala.collection.mutable.ListBuffer
import org.apache.openwhisk.core.entity.ActivationId.ActivationIdGenerator 
import java.time.Instant 
//import scala.concurrent.duration.Duration 
//class AdaptiveInvokerPoolMaintenance(var activeInvokers: IndexedSeq[InvokerHealth],var inactiveInvokers: IndexedSeq[InvokerHealth]){
class AdaptiveInvokerPoolMaintenance(var activeInvokers: ListBuffer[InvokerHealth],var inactiveInvokers: ListBuffer[InvokerHealth],logging: Logging){
  //var curActiveInvoker: InvokerHealth =  activeInvokers(0); 
  var shouldUpgradeInvoker: Boolean = false
  val makeActiveSleepInMS = 15*1000

  def checkInvokersState(): Unit = {

  }

  // used to push an invoker, if available, to go to active state from inactive state.
  def upgradeInvoker(): Option[InvokerHealth] = {
    logging.info(this,s"<adbg> <AIPM> <upgradeInvoker> inactiveInvokers.size: ${inactiveInvokers.size} ")
    if(inactiveInvokers.size > 0){
      logging.info(this,s"<adbg> <AIPM> <upgradeInvoker> YAAAy! Will get an invoker")
      var toYieldInvoker: InvokerHealth = inactiveInvokers.remove(0)
      return Some(toYieldInvoker)
    } else{
      logging.info(this,s"<adbg> <AIPM> <upgradeInvoker> No inactive-invokers present! Should use random assignment..")
      return None
    }   
  }

  def dummySleep(): Future[Unit] = {
    val sleepStart: Long = Instant.now.toEpochMilli
    Thread.sleep(makeActiveSleepInMS)
    val sleepEnd: Long = Instant.now.toEpochMilli
    val sleptTime: Long = sleepEnd - sleepStart
    logging.info(this,s"<adbg> <AIPM> <dummySleep> Slept for ${sleptTime} end: ${sleepEnd} begin: ${sleepStart} ")
    Future.successful(())
  }

  def makeInvokerActive(toActivateInvoker: InvokerHealth): Unit = {

    activeInvokers.foreach{
      curInvoker =>
      logging.info(this,s"<adbg> <AIPM> <makeInvokerActive:listInvokers> invoker: ${curInvoker.id.toInt} is currently ACTIVE.. ")
    }

    inactiveInvokers.foreach{
      curInvoker =>
      logging.info(this,s"<adbg> <AIPM> <makeInvokerActive:listInvokers> invoker: ${curInvoker.id.toInt} is currently INACTIVE.. ")
    }

    if(!activeInvokers.contains(toActivateInvoker)){
      //logging.info(this,s"<adbg> <AIPM> <makeInvokerActive> invoker: ${toActivateInvoker.id.toInt} is being made active.. and will sleep for threshold: ${makeActiveSleepInMS} ms ")
      logging.info(this,s"<adbg> <AIPM> <makeInvokerActive> invoker: ${toActivateInvoker.id.toInt} is being made active..")
      if(inactiveInvokers.contains(toActivateInvoker)){
        inactiveInvokers-=toActivateInvoker
      }
      activeInvokers+=toActivateInvoker
      /*val sleepStart: Long = Instant.now.toEpochMilli
      Thread.sleep(makeActiveSleepInMS)
      val sleepEnd: Long = Instant.now.toEpochMilli
      val sleptTime: Long = sleepEnd - sleepStart
      logging.info(this,s"<adbg> <AIPM> <makeInvokerActive> invoker: ${toActivateInvoker.id.toInt} was made active and slept for ${sleptTime} end: ${sleepEnd} begin: ${sleepStart} ")*/
    }else{
      logging.info(this,s"<adbg> <AIPM> <makeInvokerActive> invoker: ${toActivateInvoker.id.toInt} is already active.. ")
    }
  }

  // used to push an invoker, if available, to go from in-active state to active state.
  def downgradeInvoker(toDowngradeInvoker: ListBuffer[InvokerHealth]): Unit = {
    logging.info(this,s"<adbg> <AIPM> <downgradeInvoker> begin activeInvokers.size: ${activeInvokers.size} inactiveInvokers.size: ${inactiveInvokers.size}")
    toDowngradeInvoker.foreach{
      curInvoker =>
      activeInvokers-=curInvoker // might crash if there is a race condition!
      if(!(inactiveInvokers.exists( invoker => invoker == curInvoker))){
        logging.info(this,s"<adbg> <AIPM> <downgradeInvoker> apparently curInvoker: ${curInvoker.id.toInt} is not present in inactiveInvokers!")    
        inactiveInvokers+=curInvoker // this is OK..
      }
    }
    logging.info(this,s"<adbg> <AIPM> <downgradeInvoker> end activeInvokers.size: ${activeInvokers.size} inactiveInvokers.size: ${inactiveInvokers.size}")
  }

  def printSize(): Unit = {
    logging.info(this,s"<adbg> <AIPM> <printSize> activeInvokers.size: ${activeInvokers.size} ")
    logging.info(this,s"<adbg> <AIPM> <printSize> inactiveInvokers.size: ${inactiveInvokers.size} ")
  }
}
// avs --end


class AdaptiveContainerPoolBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  loadFeedFactory: FeedFactory, //avs
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, loadFeedFactory,controllerInstance) { 
//extends CommonLoadBalancer(config, feedFactory,controllerInstance) // avs added loadFeedFactory
  private var toUseProactiveInvokerId = 0; // avs //will be helpful for round-robin
  //import loadBalancer.allInvokers //avs

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  override protected def emitMetrics() = {
    super.emitMetrics()
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_BLACKBOX,
      schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.managedInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Offline))
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  /** State needed for scheduling. */
  val schedulingState = AdaptiveContainerPoolBalancerState()(adaptiveLbConfig)

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[AdaptiveContainerPoolBalancerState.updateInvokers]] and [[AdaptiveContainerPoolBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState,aIT)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
  override def clusterSize: Int = schedulingState.clusterSize

  /** 1. Publish a message to the loadbalancer */

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)

    var beginInstant: Long = Instant.now.toEpochMilli;
    var proactiveBegin: Long = 0
    var checkEnd: Long = 0;
    var proactiveEndInstat: Long = 0
    var endInstant: Long =0 
    val chosen = if (invokersToUse.nonEmpty) {
      val hash = AdaptiveContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
      // avs --begin
      /*val homeInvoker = hash % invokersToUse.size
      val stepSize = stepSizes(hash % stepSizes.size) */
      // to make it round-robin, will update the invoker used for previous invocation, and loopback later.
      val homeInvoker = (toUseProactiveInvokerId)%invokersToUse.size
      val stepSize = 1
      // avs --end
      logging.info(this,s"<adbg> <ACPB> <publish> Calling schedule for activation ${msg.activationId} for '${msg.action.asString}' ($actionType) ") // avs
      //val invoker: Option[(InvokerInstanceId, uselessMetadata, Boolean)] = AdaptiveContainerPoolBalancer.schedule(
      val invoker: Option[(allocMetadata,Boolean)] = AdaptiveContainerPoolBalancer.schedule(
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        invokersToUse,
        schedulingState.invokerSlots,
        action.limits.memory.megabytes,
        //homeInvoker,stepSize, // avs 
        schedulingState.curInvokerPoolMaintenance, //avs
        action.name.asString,
        cIC, gUIFA, gaI,nTUI)

      invoker.foreach {
        case (_,true) =>
          //logging.info(this,s"<adbg> 1. obtainedMetaData.id: ${obtainedMetaData.id}")
          val metric =
            if (isBlackboxInvocation)
              LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else
              LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }
      invoker.map(_._1)
    } else {
      None
    }

    // avs --begin
    var curInstant: Long = Instant.now.toEpochMilli
    if( (curInstant-lastUpdatedTime ) > scanIntervalInMS){
        lastUpdatedTime = curInstant
        logging.info(this,s"<adbg> <ACPB> <publish> During schedule of activation ${msg.activationId} for '${msg.action.asString}' ($actionType), doing background maintenance") // avs
        
        // Should do this organically too, but how? Maybe, if am chekcing all the invokers, should update the flag..
        //if(needToUpgradeInvoker(activeInvokersBuffer)){
        if(nTUI(schedulingState.curInvokerPoolMaintenance.activeInvokers)){
          schedulingState.curInvokerPoolMaintenance.upgradeInvoker()
        }else{
          //var invokersToDowngrade: ListBuffer[InvokerHealth] = needToDowngradeInvoker(activeInvokers)
          schedulingState.curInvokerPoolMaintenance.downgradeInvoker(needToDowngradeInvoker(schedulingState.curInvokerPoolMaintenance.activeInvokers))
        }
    }else if(schedulingState.curInvokerPoolMaintenance.shouldUpgradeInvoker){
      schedulingState.curInvokerPoolMaintenance.upgradeInvoker()
    }
    // avs --end

    chosen
      .map { curReqAllocMetadata =>
        val invoker = curReqAllocMetadata.allocInvokerId
        // TODO: Should somehow ensure toUseProactiveInvoker will be upgraded and is active! 
        toUseProactiveInvokerId = (invoker.toInt+1)%schedulingState.managedInvokers.size //curInvokerPoolMaintenance.activeInvokers.size //avs 
        logging.info(
          this,
          s"activation ${msg.activationId} for '${msg.action.asString}' ($actionType) by namespace '${msg.user.namespace.name.asString}' with memory limit ${action.limits.memory.megabytes}MB assigned to (schedule) $invoker and toUseProactiveInvokerId: ${toUseProactiveInvokerId}.")        

        val activationResult = setupActivation(msg, action, invoker)
        val origActivMetadata = sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)

        // avs --begin
        proactiveBegin = Instant.now.toEpochMilli        

        var proactivInvkr1: InvokerInstanceId = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr1; 
        var proactivInvkr2: InvokerInstanceId = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr2; 
        var proactivInvkr1NumReqs = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr1NumReqs; 
        var proactivInvkr2NumReqs = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr2NumReqs; 
        var curInvkrNumReqs = 0; var curProactivInvkr: InvokerInstanceId = curReqAllocMetadata.allocInvokerId;
        for(curIter <- 1 to 2){
            if(curIter==1){
              curInvkrNumReqs = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr1NumReqs; 
              curProactivInvkr = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr1; 
            }else{
              curInvkrNumReqs = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr2NumReqs; 
              curProactivInvkr = curReqAllocMetadata.curReqIsrpnMetadata.proactivInvkr2;               
            }
            logging.info(this,s"<adbg> <PCS:1> action: ${action.name.asString} curIter: ${curIter} curInvkr: ${curProactivInvkr.toInt} curInvkrNumReqs: ${curInvkrNumReqs} ")

            for(curDummyReqNum <- 1 to curInvkrNumReqs){
              val proactiveMsg = ActivationMessage(
                // Use the sid of the InvokerSupervisor as tid
                transid = transid,
                action = action.fullyQualifiedName(true),
                // Use empty DocRevision to force the invoker to pull the action from db all the time
                revision = DocRevision.empty,
                user = msg.user,
                // Create a new Activation ID for this activation
                activationId = new ActivationIdGenerator {}.make(),
                rootControllerIndex = msg.rootControllerIndex, //controllerInstance,
                blocking = false,
                content = None,
                proactiveSpawning = true
              )  
              if(curIter==1){
                if(curProactivInvkr.toInt < schedulingState.managedInvokers.size){
                  val tempInvoker_toMakeActive: InvokerHealth = schedulingState.managedInvokers(curProactivInvkr.toInt)
                  //schedulingState.curInvokerPoolMaintenance.makeInvokerActive(tempInvoker_toMakeActive)
                }else{
                  logging.info(this,s"<adbg> <PCS:ERROR> action: ${action.name.asString} curActivation: ${msg.activationId} wants to issue a new activation: ${proactiveMsg.activationId} to invoker: ${curProactivInvkr.toInt} but it's not found in managedInvokers!! HANDLE it..")                        
                }            
              }
              val proActiveResult = setupActivation(proactiveMsg, action, curProactivInvkr)
              sendActivationToInvoker(messageProducer, proactiveMsg, curProactivInvkr).map(_ => proActiveResult)          
              logging.info(this,s"<adbg> <PCS:2.0> action: ${action.name.asString} curIter: ${curIter} curActivation: ${msg.activationId} issued a new activation: ${proactiveMsg.activationId} in invoker: ${curProactivInvkr.toInt}")            
            }                        
        }
        proactiveEndInstat = Instant.now.toEpochMilli // avs
        endInstant = proactiveEndInstat
        logging.info(this,s"<adbg> <PCS:DONE> action: ${action.name.asString} dispatching activation: ${msg.activationId} time-taken(end-begin): ${endInstant-beginInstant} proactive: ${endInstant - proactiveBegin}")
        // avs --end
        origActivMetadata 
      }
      .getOrElse {
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(this, s"failed to schedule $actionType action, invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  override val invokerPool =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memory.toMB.toInt))
  }
}

object AdaptiveContainerPoolBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messagingProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor))
      }

    }
    new AdaptiveContainerPoolBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      createLoadFeedFactory(whiskConfig, instance), //avs
      invokerPoolFactory)
  }

  def requiredProperties: Map[String, String] = kafkaHosts

  /** Generates a hash based on the string representation of namespace and action */
  def generateHash(namespace: EntityName, action: FullyQualifiedEntityName): Int = {
    (namespace.asString.hashCode() ^ action.asString.hashCode()).abs
  }

  /** Euclidean algorithm to determine the greatest-common-divisor */
  @tailrec
  def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

  /** Returns pairwise coprime numbers until x. Result is memoized. */
  def pairwiseCoprimeNumbersUntil(x: Int): IndexedSeq[Int] =
    (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
      if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
        primes :+ cur
      } else primes
    })

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param invokers a list of available invokers to search in, including their state
   * @param dispatched semaphores for each invoker to give the slots away from
   * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
   * @param index the index to start from (initially should be the "homeInvoker"
   * @param step stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */

// avs --begin
  //@tailrec
    def schedule(
      maxConcurrent: Int,
      fqn: FullyQualifiedEntityName,
      invokers: IndexedSeq[InvokerHealth],
      dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
      slots: Int,
      curInvokerPoolMaintenance: AdaptiveInvokerPoolMaintenance,
      actionName: String,
      checkInvokerCapacity: (InvokerHealth,String) => Boolean, //avs
      getUsedInvokerForAction :(String) => Option[(InvokerInstanceId,allocMetadata)],
      getActiveInvoker: (String, ListBuffer[InvokerHealth]) => Option[(InvokerInstanceId,allocMetadata)],
      needToUpgradeInvoker: (ListBuffer[InvokerHealth])=> Boolean       
    )(implicit logging: Logging, transId: TransactionId): Option[(allocMetadata, Boolean)] = { //Option[(InvokerInstanceId, uselessMetadata,Boolean)] = {
    //)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, uselessMetadata,Boolean)] = {  
      //val numInvokers = invokers.size

      var activeInvokers: ListBuffer[InvokerHealth] = curInvokerPoolMaintenance.activeInvokers;
      // 1. Check whether invokers where I have my containers can accommodate me?
      // 2. Ok, then can I open a new invoker in an existing node?
      // 3. Shucks, ok, will move a node from inactive state to active state.
      //var chosenInvoker = getExistingContainer(actionName)

      logging.info(this,s"<adbg> <schedule> 0.0 ") // avs
      val invoker: Option[(InvokerInstanceId,allocMetadata)] = getUsedInvokerForAction(actionName) //curInvokerPoolMaintenance.getExistingContainer(actionName)
      /*
      // If it has come here, will default to backup logic of OpenWHisk, i.e. assign randomly if possible
      val healthyInvokers = invokers.filter(_.status.isUsable)
      if (healthyInvokers.nonEmpty) {
        logging.info(this,s"<adbg> <schedule> 2.1 <found-A-healthy-invoker> ") // avs
        // Choose a healthy invoker randomly
        val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
        dispatched(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
        logging.info(this,s"<adbg> <schedule> 2.2 <found-A-healthy-invoker> invoker: ${random.toInt}") // avs
        logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
        val obtainedMetaData: uselessMetadata = new uselessMetadata(random,true)
        return Some(random,obtainedMetaData, true)
      } else {
        None
      }  
      */    
      invoker match{
        case Some((chosenInvoker,curReqAllocMetadata))=>
        //case (chosenInvoker,curReqAllocMetadata) =>
          logging.info(this,s"<adbg> <schedule> 1.0, got an invoker from getUsedInvokerForAction ") // avs
          //val obtainedMetaData: uselessMetadata = new uselessMetadata(chosenInvoker,true)
          //return Some(chosenInvoker,obtainedMetaData,true) //,true)
          return Some(curReqAllocMetadata,true)
        case _ =>
          logging.info(this,s"<adbg> <schedule> 1.1, Did-NOT get an invoker from getUsedInvokerForAction ") // avs
          //return None
      }      
      return None
  }
}
// avs --end

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers all of the known invokers in the system
 * @param _managedInvokers all invokers for managed runtimes
 * @param _blackboxInvokers all invokers for blackbox runtimes
 * @param _managedStepSizes the step-sizes possible for the current managed invoker count
 * @param _blackboxStepSizes the step-sizes possible for the current blackbox invoker count
 * @param _invokerSlots state of accessible slots of each invoker
 */
case class AdaptiveContainerPoolBalancerState(
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedStepSizes: Seq[Int] = AdaptiveContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  private var _blackboxStepSizes: Seq[Int] = AdaptiveContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  protected[loadBalancer] var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
    IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
  private var _clusterSize: Int = 1)(
  adaptiveLbConfig: AdaptiveContainerPoolBalancerConfig =
    loadConfigOrThrow[AdaptiveContainerPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {

  // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
  // 1.0 and 2.0.
  // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
  // means, that there is no differentiation between managed and blackbox invokers.
  // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
  // specified in config and adapted to the managed fraction.
  private val managedFraction: Double = 1.0 //Math.max(0.0, Math.min(1.0, adaptiveLbConfig.managedFraction)) //avs
  private val blackboxFraction: Double = 1.0 //Math.max(1.0 - managedFraction, Math.min(1.0, adaptiveLbConfig.blackboxFraction)) //avs
  logging.info(this, s"managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
    TransactionId.loadbalancer)

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
  def managedStepSizes: Seq[Int] = _managedStepSizes
  def blackboxStepSizes: Seq[Int] = _blackboxStepSizes
  def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots
  def clusterSize: Int = _clusterSize

// avs --begin
  var _activeInvokersBuffer: ListBuffer[InvokerHealth]  = new mutable.ListBuffer[InvokerHealth]
  var _inactiveInvokersBuffer: ListBuffer[InvokerHealth]  = new mutable.ListBuffer[InvokerHealth]
  var _curInvokerPoolMaintenance = new AdaptiveInvokerPoolMaintenance(_activeInvokersBuffer,_inactiveInvokersBuffer,logging)

  def curInvokerPoolMaintenance: AdaptiveInvokerPoolMaintenance = _curInvokerPoolMaintenance
  def activeInvokersBuffer: ListBuffer[InvokerHealth] = _activeInvokersBuffer 

  var aipmInit: Boolean = false
  var _allInvokersUsed: Boolean = false
  val numProactiveContsToSpawn = 2
  val curInvokerProactiveContsToSpawn = 1

// avs --end
  /**
   * @param memory
   * @return calculated invoker slot
   */
  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    val newTreshold = if (memory / _clusterSize < MemoryLimit.minMemory) {
      logging.warn(
        this,
        s"registered controllers: ${_clusterSize}: the slots per invoker fall below the min memory of one action.")(
        TransactionId.loadbalancer)
      MemoryLimit.minMemory
    } else {
      memory / _clusterSize
    }
    newTreshold
  }

  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  def updateInvokers(
    newInvokers: IndexedSeq[InvokerHealth],
    //checkInvokerCapacity: (InvokerInstanceId) => Boolean
    addInvokerTracking: (InvokerHealth,Int,Int) => Unit): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    // for small N, allow the managed invokers to overlap with blackbox invokers, and
    // further assume that blackbox invokers << managed invokers
    val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

    _invokers = newInvokers
    _managedInvokers = _invokers.take(managed)
    _blackboxInvokers = _invokers.takeRight(blackboxes)

    // avs --begin
    var tempNumCores = 4;
    _invokers.foreach{ curInvoker =>
      //curInvoker.myStats.updateInvokerResource(4,8*1024) // 4 cores, 8GB
      addInvokerTracking(curInvoker,tempNumCores,8*1024)
      logging.info(this,s"<adbg> in <updateInvokers> curInvoker: ${curInvoker.id.toInt}")
    }

    // avs --end

    if (oldSize != newSize) {
      _managedStepSizes = AdaptiveContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
      _blackboxStepSizes = AdaptiveContainerPoolBalancer.pairwiseCoprimeNumbersUntil(blackboxes)

      if (oldSize < newSize) {
        // Keeps the existing state..
        _invokerSlots = _invokerSlots ++ _invokers.drop(_invokerSlots.length).map { invoker =>
          new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
        }
      }
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes")(
      TransactionId.loadbalancer)

// avs --begin
    if(!aipmInit){
      logging.info(this,s"<adbg>, now populating, curInvokerPoolMaintenance object! ")
      _invokers.foreach {
        curInvoker =>
        _activeInvokersBuffer += curInvoker // TODO-Testing: Check whether the code works without having any active Invokers, to begin with.
        _inactiveInvokersBuffer += curInvoker
      }    
      
      _curInvokerPoolMaintenance.activeInvokers = _activeInvokersBuffer
      _curInvokerPoolMaintenance.inactiveInvokers = _inactiveInvokersBuffer
      _curInvokerPoolMaintenance.printSize()
      logging.info(this,s"<adbg>, DONE populating, curInvokerPoolMaintenance object! ")
      aipmInit = true      
    }
// avs --end       
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      _clusterSize = actualSize
      _invokerSlots = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
      }
      logging.info(this, s"loadbalancer cluster size changed to $actualSize active nodes.")(TransactionId.loadbalancer)
    }
  }
}

/**
 * Configuration for the cluster created between loadbalancers.
 *
 * @param useClusterBootstrap Whether or not to use a bootstrap mechanism
 */
//case class ClusterConfig(useClusterBootstrap: Boolean)

/**
 * Configuration for the sharding container pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 * @param timeoutFactor factor to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + 1m)
 */
case class AdaptiveContainerPoolBalancerConfig(managedFraction: Double, blackboxFraction: Double, timeoutFactor: Int)

// avs --end
