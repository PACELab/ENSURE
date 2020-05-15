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

package org.apache.openwhisk.core.containerpool

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import org.apache.openwhisk.common.{AkkaLogging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Try
import scala.collection.mutable.ListBuffer //avs
import org.apache.openwhisk.core.invoker.InvokerReactive.ActiveLoadResp // avs
import java.time.Instant //avs

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState

case class WorkerData(data: ContainerData, state: WorkerState)
case class MutableTriplet[A,B,C](var _1: A, var _2: B, var _3: C) {} //avs
//implicit def doublet_to_tuple[A,B](db: MutableTriplet[A,B]) = (db._1, db._2)

// avs --begin
class contStatsData(var cpuShares: Int,val trackContId:Int){
  var numTimesUsed: Int = 0
  var curWindowUsed: Int  = 0
}

class toRelayActionStats(val actionName: String,val avgLatency: Long, val numConts: Int, val numWindowConts: Int){
}

class TrackFunctionStats(
  var actionName: String, 
  myStandaloneRuntime: Long, 
  private var myAction: ExecutableWhiskAction,
  private val defaultCpuShares: Int,
  private val curId: TransactionId, 
  private val logging: AkkaLogging,
  //private val totalCpuShares: Int,
  //val cpuSharesPool:immutable.Map[ActorRef, funcConfigTracking]
  ) {

  import ContainerPool.cpuSharesCheck
  //import ContainerPool.cpuSharesPool
  import ContainerPool.getActionType
  import ContainerPool.printAllCpuShares

  private var cumulRuntime: Long = 0;
  private var numInvocations: Long = 0;
  //private var curCpuShares: Int = 0;
  private var curCpuSharesUsed: Int = -1;
  private var allCpuShares: ListBuffer[Int] = new mutable.ListBuffer[Int];
  //private var myContainers: ListBuffer[Container] =  new ListBuffer[Container]();
  //private var myContainers = mutable.Map.empty[Container, Int]
  var myContainers = mutable.Map.empty[Container, contStatsData]
  private var updateCount_Flag: Boolean = false;
  private var numViolations: Int = 0

  private var myActionType = getActionType(actionName)
  private val latencyThreshold : Double  = 1.10;
  private val violationThreshold: Int = 1;
  private val default_cpuSharesUpdate_Threshold: Int = if(myActionType=="ET") 5 else 3
  private var curCpuSharesUpdate_Threshold : Int = default_cpuSharesUpdate_Threshold;
  private var shouldEaseup: Boolean = false;

  private val perIterIncrement = if(myActionType=="ET") 128 else 64
  val maxCpuShares = if(myActionType=="ET") 768 else 256

  private var prevWindowTime:Long = Instant.now.toEpochMilli
  private var activityWindowSize = 5*1000; // 10 milliseconds
  if(activityWindowSize < myStandaloneRuntime) activityWindowSize = (2*myStandaloneRuntime.toInt)

  private var curWindowNumConts = 0;

  private var numReqsProcessed = 0 // should be zero, but to debug have set it to 1.
  private var trackSharesUsed = mutable.Map.empty[Int,Int] // <num-shares>,<num-times-used>
  trackSharesUsed = trackSharesUsed + (defaultCpuShares -> 0)

  var curCpuShares = if(myActionType=="ET") defaultCpuShares else maxCpuShares //maxCpuShares
  var prevSharesUsed = curCpuShares // defaultCpuShares
  allCpuShares+= defaultCpuShares // added as part of consturctor.

  var latWindowSize = 10
  var latWindow: ListBuffer[Long] = ListBuffer.fill(latWindowSize)(0) //var latWindow: ListBuffer[Long] = new mutable.ListBuffer[Long]
  var latWindowIdx: Int = 0 

// Begin -- Merged from funcConfigTracking
  def getCurContCpuShares(container: Container): Int = {
    myContainers.get(container) match {
      case Some(myContStats) => 
        //logging.info(this, s"<adbg> <funcConfigTracking> <getCurCpuShares> for action: ${actionName} myContStats.cpuShares: ${myContStats.cpuShares} and id: ${myContStats.trackContId}")
        myContStats.cpuShares
      case None => 
        logging.info(this, s"<adbg> <TFS> <getCurContCpuShares> for action: ${actionName} container missing. HANDLE it!")
        0
    }
  }

  def getCurContID(container: Container): Int = {
    myContainers.get(container) match {
      case Some(myContStats) => 
        myContStats.trackContId
      case None => 
        logging.info(this, s"<adbg> <TFS> <getCurContID> for action: ${actionName} container missing. HANDLE it!")
        0
    }
  }

  def setCurContCpuShares(container: Container,toSetCpuShares: Int): Unit = {
    myContainers.get(container) match {
      case Some(myContStats) => 
        myContStats.cpuShares = toSetCpuShares
        //logging.info(this, s"<adbg> <funcConfigTracking> <setCurCpuShares> for action: ${actionName} myContStats.cpuShares (updated): ${myContStats.cpuShares} and id: ${myContStats.trackContId} and is used: ${myContStats.numTimesUsed}")
      case None => 
        logging.info(this, s"<adbg> <TFS> <setCurCpuShares> for action: ${actionName} container missing. HANDLE it!")
    
    }
  }

  def addContainer(container: Container,trackContId:Int,dummyReq:Int): Int = {
    //myContainers+= container;    
    myContainers.get(container) match {
      case Some(myContStats) => 
        if(dummyReq==0){
          myContStats.numTimesUsed+=1
          myContStats.curWindowUsed+=1                          
        }
        //logging.info(this, s"<adbg> <TFS> <addContainer-1> for action: ${actionName} adding a container curCpuShares: ${curCpuShares} trackContId: ${trackContId} defaultCpuShares: ${defaultCpuShares}")
        logging.info(this, s"<adbg> <TFS> <addContainer-1> for action: ${actionName} trackContId: ${myContStats.trackContId} dummyReq: ${dummyReq} numTimesUsed: ${myContStats.numTimesUsed} curWindowUsed: ${myContStats.curWindowUsed} ")
        //setCurContCpuShares(container,curCpuShares)
        trackContId // not updating the trackContId
      case None =>         
        //myContainers = myContainers + (container -> new contStatsData(defaultCpuShares,trackContId) )
         // so that it starts using the apt CPU shares.
        container.updateCpuShares(curId,curCpuShares); myContainers = myContainers + (container -> new contStatsData(curCpuShares,trackContId) )
        logging.info(this, s"<adbg> <TFS> <addContainer-2> for action: ${actionName} adding a container curCpuShares: ${curCpuShares} dummyReq: ${dummyReq} trackContId: ${trackContId+1} defaultCpuShares: ${defaultCpuShares}")
        var tempContStats = myContainers(container)
        if(dummyReq==0){
          tempContStats.numTimesUsed+=1;
          tempContStats.curWindowUsed+=1
        }
        trackContId+1 // updating the trackContId
    }
  }

  def removeContainer(container: Container): Unit = {
    myContainers.get(container) match {
      case Some(myContStats) => 
        curCpuSharesUsed = if(curCpuSharesUsed>curCpuSharesUpdate_Threshold) curCpuSharesUsed-curCpuSharesUpdate_Threshold else 0    
        logging.info(this, s"<adbg> <TFS> <removeContainer> for action: ${actionName} removing a container (${myContStats.trackContId}) which was used ${myContStats.numTimesUsed} #times")
        myContainers = myContainers - container
      case None => 
        logging.info(this, s"<adbg> <TFS> <removeContainer> for action: ${actionName}. Unfortunately the container wasn't tracked! HANDLE it!")
        //myContainers = myContainers + (container -> 0) // will reset it, but doesnt matter.
    }

    /*if(myContainers.size==0){
      logging.info(this, s"<adbg> <TFS> <removeContainer> for action: ${actionName} don't have any containers. Will reset curCpuSharesto defaultCpuShares: ${defaultCpuShares} ")
      curCpuShares = defaultCpuShares // can set this to most-used-cpu-shares
    }*/
  }


  def printAllContainers(): Unit = {
    var curBatch_minCpuShares = maxCpuShares; var numConts = 0
    myContainers.keys.foreach{ curCont =>
      var curContData: contStatsData = myContainers(curCont)
      logging.info(this,s"<adbg><TFS><printAllContainers> ${actionName} ${curContData.trackContId} ${curContData.cpuShares}")
      if(curBatch_minCpuShares > curContData.cpuShares)
        curBatch_minCpuShares = curContData.cpuShares
      numConts+=1
    } 
    curCpuShares = if(numConts!=0) curBatch_minCpuShares else defaultCpuShares
  }

  def getCurCpuShares(): Int = {
    curCpuShares
  }

  def accumAllCpuShares(): Int = {
    var sumCpuShares = 0
    myContainers.keys.foreach{ curCont =>
      var curContData: contStatsData = myContainers(curCont)
      logging.info(this,s"<adbg><TFS><accumAllCpuShares> ${actionName} ${curContData.trackContId} ${curContData.cpuShares}")
      sumCpuShares = sumCpuShares + curContData.cpuShares
    } 
    sumCpuShares
  }

// End -- Merged from funcConfigTracking
  def dummyCall(): Unit = {
    //logging.info(this, s"<adbg> <TFS> <dummyCall> for action: ${actionName} ")
  }

  def getDefaultCpuShares(): Int = {
    defaultCpuShares
  }

  def getCurTxnId(): TransactionId = {
    curId
  }
  // Pending:
  // Should use average/window to trigger?
  // Updating curCpuSharesUsed when a container is removed (done).
  // Adding some sort of lock so that only one container will trigger the cpuSharesUpdate. However, should be cautious to ensure that other containers wont be wrecked!
  //    Answer: Is this really an issue with Actors (for now, assuming that each call (from container) to Actor (c-pool) will be eventually run and there won't be race-conditions as such. Should read up on Actors and revisit it later)  
  // Should also make any new container to use the "curCpuShares" -- done

  // checkCpuShares is a bit more robust algorithm. 
  // It increases cpu-shares for both ET and MP.
  // It keeps track of CPU shares as a finite resource in a node (i.e. 1024 * num-cores) and also takes care of reducing CPU shares when needed.
  def checkCpuShares(movingAvgLatency: Long): Unit = {
    if(updateCount_Flag)
      curCpuSharesUsed+=1

    trackSharesUsed.get(curCpuShares) match {
      case Some(curSharesCount) => trackSharesUsed(curCpuShares)+=1
      case None => trackSharesUsed = trackSharesUsed + (curCpuShares->1)
    }

    if(movingAvgLatency> (latencyThreshold * myStandaloneRuntime) ){
      numViolations+=1
      logging.info(this, s"<adbg> <TFS> <checkCpuShares> 1. for action: ${actionName} movingAvgLatency: ${movingAvgLatency} numReqsProcessed: ${numReqsProcessed} numViolations: ${numViolations} vt: ${violationThreshold}")      
    }else{
      logging.info(this, s"<adbg> <TFS> <checkCpuShares> 2. for action: ${actionName} movingAvgLatency: ${movingAvgLatency} numReqsProcessed: ${numReqsProcessed} numViolations: ${numViolations} vt: ${violationThreshold}")      
    }

    if( numViolations >= violationThreshold ){
        numViolations = 0

      //logging.info(this, s"<adbg> <TFS> <checkCpuShares> for action: ${actionName} movingAvgLatency: ${movingAvgLatency} is greater than 120% of myStandaloneRuntime: ${myStandaloneRuntime}; cumulRuntime: ${cumulRuntime} and #invocations: ${numInvocations}")  
      // all hell will break loose if multiple containers of the same type call this at the same time!
      var curNumConts = if(numContainerTracked()!=0) numContainerTracked() else 1;
      var avgNumtimeUsed = (curCpuSharesUsed/(curNumConts))
      if( (curCpuSharesUsed == -1) || (avgNumtimeUsed >=curCpuSharesUpdate_Threshold) ){

        if(curCpuShares<maxCpuShares){
          prevSharesUsed = curCpuShares
          var tempCpuShares = curCpuShares+perIterIncrement;
          //logging.info(this, s"<adbg> <TFS> <checkCpuShares> for action: ${actionName} curCpuShares: ${curCpuShares} will be CHANGED to ${tempCpuShares} which we should infer is not as big as the max-cpu-shares: ${maxCpuShares}")  
          var toIncrememnetShares = cpuSharesCheck(logging,perIterIncrement,curNumConts,actionName)
          var couldBeCpuShares = curCpuShares + toIncrememnetShares
          
          if(toIncrememnetShares < perIterIncrement) // ok, cant increase all the way..
            curCpuShares = couldBeCpuShares
          else // ok, CAN increase all the way..
            curCpuShares = tempCpuShares

          if(curCpuShares<=defaultCpuShares)
            curCpuShares = defaultCpuShares
          else if(curCpuShares>maxCpuShares) 
            curCpuShares = maxCpuShares

          updateCount_Flag = false; curCpuSharesUsed = 0
          var curBatch_minCpuShares = maxCpuShares
          if(toIncrememnetShares>0){
            myContainers.keys.foreach{ cont => 
              var tempCpuShares = getCurContCpuShares(cont)
              var toSetCpuShares = tempCpuShares + toIncrememnetShares

              if(toSetCpuShares < defaultCpuShares)
                toSetCpuShares = defaultCpuShares
              else if(toSetCpuShares > maxCpuShares)
                toSetCpuShares = maxCpuShares

              cont.updateCpuShares(curId,toSetCpuShares)  // only doing this for experiments to get the baseline.
              setCurContCpuShares(cont,toSetCpuShares)
              // overkill to do it every time, but ensures that will only be updated on actually updating cpuShares
              updateCount_Flag = true;
              if(curBatch_minCpuShares > toSetCpuShares)
                curBatch_minCpuShares = toSetCpuShares
            }
          }

          if(updateCount_Flag){
            curCpuShares = curBatch_minCpuShares // this way, I will give atleast the minCpuShare of existing batch to the new container, if one is spawned. also, would ensure I won't get stuck at maxCpuShares if it hit there once!

            curCpuSharesUsed = 0;
            if(shouldEaseup){ 
              curCpuSharesUpdate_Threshold = default_cpuSharesUpdate_Threshold * 3; // backing off, since the system is likely running at it's peak.
            }else{
              curCpuSharesUpdate_Threshold = default_cpuSharesUpdate_Threshold;
            }

            allCpuShares+= curCpuShares
            myAction.limits.iVals.myInferredConfig.mostusedCpuShares = trackSharesUsed.keysIterator.max //trackSharesUsed.maxBy { case (key, value) => value }
            myAction.limits.iVals.myInferredConfig.numTimesUpdated = myAction.limits.iVals.myInferredConfig.numTimesUpdated+1
            //printAllCpuShares(logging)              
            logging.info(this, s"<adbg> <TFS> <checkCpuShares> for action: ${actionName} update curShares: ${curCpuShares} prevSharesUsed: ${prevSharesUsed} and couldBeCpuShares: ${couldBeCpuShares} and tempCpuShares: ${tempCpuShares}. ")
            logging.info(this, s"<adbg> <TFS> <checkCpuShares> action: ${actionName} shouldEaseup: ${shouldEaseup} and on average will wait for ${curCpuSharesUpdate_Threshold} mostusedCpuShares: ${myAction.limits.iVals.myInferredConfig.mostusedCpuShares}, avgNumtimeUsed: ${avgNumtimeUsed} numReqsProcessed: ${numReqsProcessed} and curNumConts: ${curNumConts}")                
          }
          //logging.info(this, s"<adbg> <TFS> <checkCpuShares> response from getCpuSharesFor is ${couldBeCpuShares} mostusedCpuShares: ${myAction.limits.iVals.myInferredConfig.mostusedCpuShares} numTimesUpdated: ${myAction.limits.iVals.myInferredConfig.numTimesUpdated}") 
        }
        else{
          logging.info(this, s"<adbg> <TFS> <checkCpuShares> for action: ${actionName} curCpuShares: ${curCpuShares} is atleast as big as the max-cpu-shares: ${maxCpuShares}. NOT going to UPDATE the cpushares")  
          curCpuSharesUsed = 0;
        }
      }else{
        updateCount_Flag = true; // if it is coming here, it should be updated..
        logging.info(this, s"<adbg> <TFS> <checkCpuShares> for action: ${actionName}. Even though the latency is greater than the threshold, latest updated cpushares is used: ${curCpuSharesUsed} across ${curNumConts} and it has been used on average ${avgNumtimeUsed}. Waiting for it to be used ${curCpuSharesUpdate_Threshold} on an average before next round of updates")          
      }
    }else{
      logging.info(this, s"<adbg> <TFS> <checkCpuShares> Nothing to do folks! numViolations: ${numViolations} and violationThreshold: ${violationThreshold}")      
    }
  }  

  def addRuntime(curRuntime: Long): Unit = {  
    cumulRuntime+= curRuntime
    numReqsProcessed+=1
    //logging.info(this, s"<adbg> <TFS> <addRuntime> for action: ${actionName} cumulRuntime: ${cumulRuntime} curRuntime: ${curRuntime} and numInvocations: ${numInvocations}")
    //dummyCall()
    var movingAvgLatency = getWindowAvgRuntime(curRuntime)
    checkCpuShares(movingAvgLatency)
    if(curCpuSharesUsed==(curCpuSharesUpdate_Threshold-1)){
      printAllCpuShares(logging)
    }
  }

  def getCurWindowNumConts(): Int = {
    var curInst: Long = Instant.now.toEpochMilli
    if( (curInst- prevWindowTime)> activityWindowSize){
      var tempCurWindowContCount = 0
      myContainers.keys.foreach{ curCont =>
        var curContData: contStatsData = myContainers(curCont)
        logging.info(this,s"<adbg><TFS><gcwnc-1> ${actionName} ${curContData.trackContId} curWindowUsed: ${curContData.curWindowUsed} numTimesUsed: ${curContData.numTimesUsed}")
        if(curContData.curWindowUsed>0)
          tempCurWindowContCount+=1
        curContData.curWindowUsed = 0
      }
      logging.info(this,s"<adbg><TFS><gcwnc-2> ${actionName} updating curWindowNumConts: ${curWindowNumConts} to ${tempCurWindowContCount}")
      curWindowNumConts = tempCurWindowContCount
      prevWindowTime = curInst
    }else{
      logging.info(this,s"<adbg><TFS><gcwnc-3> ${actionName} prevWindowTime: ${prevWindowTime} curInst: ${curInst} ")
    }
    curWindowNumConts
  }

  /*def getCurWindowNumConts(): Int = {
    if(myContainers.size==0) 1
    else myContainers.size    
  } */

  def numContainerTracked(): Int= {
    //logging.info(this, s"<adbg> <TFS> <addContainer> for action: ${actionName} #containers are ${myContainers.size}")
    if(myContainers.size==0) 1
    else myContainers.size
  }

  var runningCount: Long = 0  

  def getWindowAvgRuntime(curRuntime: Long): Long = {

      latWindow(latWindowIdx) = curRuntime
      latWindowIdx = (latWindowIdx+1)%latWindowSize
      var cumulSum: Long = latWindow.reduce((x,y)=> x+y )  
      runningCount+= 1

      var movingAvgLatency: Long = 0
      if(runningCount>0){
        if(runningCount>latWindowSize) // overflow, so we have removed N-latWindowSize'th request and added (N+1)th request, i.e. we only have latWindowSize number of requests..
          movingAvgLatency = cumulSum/latWindowSize
        else // We haven't yet populated the window, so we'll use first runningCount requests and use it as denominator. Can also use latWindowIdx though..
          movingAvgLatency = cumulSum/runningCount
      }   
    logging.info(this,s"\t <adbg> <TFS> action: ${actionName} latency: ${curRuntime} count: ${runningCount} cumulSum: ${cumulSum} movingAvgLatency: ${movingAvgLatency} ")     
    movingAvgLatency
  }

  def getAvgRuntime(): Long ={
    if(numReqsProcessed!=0) {
      logging.info(this, s"<adbg> <TFS> <getAverageRuntime> for action: ${actionName} cumulRuntime: ${cumulRuntime} and numReqsProcessed: ${numReqsProcessed} avgLatency: ${cumulRuntime/numReqsProcessed}")
      cumulRuntime/numReqsProcessed
    }else{
      0
    }
  }
}
// avs --end

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 * @param poolConfig config for the ContainerPool
 */
class ContainerPool(childFactory: ActorRefFactory => ActorRef,
                    feed: ActorRef,
                    prewarmConfig: List[PrewarmingConfig] = List.empty,
                    poolConfig: ContainerPoolConfig,
                    relayActionStats: ActiveLoadResp // avs
                  )
    extends Actor {
  import ContainerPool.memoryConsumptionOf
  import ContainerPool.getCurActionStats // avs
  import ContainerPool.getCurActionConts // avs
  import ContainerPool.getCurActionWindowConts // avs

  //import ContainerPool.cpuSharesConsumptionOf
  import ContainerPool.cpuSharesCheck
  implicit val logging = new AkkaLogging(context.system.log)

  var freePool = immutable.Map.empty[ActorRef, ContainerData]
  var busyPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmedPool = immutable.Map.empty[ActorRef, ContainerData]

  //avs --begin 
   
  //var cpuSharesPool = immutable.Map.empty[ActorRef, Int]
  import ContainerPool.cpuSharesPool // protected[containerpool]  var cpuSharesPool = immutable.Map.empty[ActorRef, funcConfigTracking]
  import ContainerPool.appIsoLatency
  var canUseCore = -1; 
  //var totalCpuShares = 4*1024//1024; // WARNING: Should move this to poolConfig and to make it inferrable.
  // avs --end

  // If all memory slots are occupied and if there is currently no container to be removed, than the actions will be
  // buffered here to keep order of computation.
  // Otherwise actions with small memory-limits could block actions with large memory limits.
  var runBuffer = immutable.Queue.empty[Run]
  val logMessageInterval = 10.seconds
  
  // avs --begin
  var trackContId = 0;
  def addFunctionRuntime(functionName: String): Unit = {
    appIsoLatency = appIsoLatency + (functionName -> 5*60*1000)      
  }  
  // avs --end

  prewarmConfig.foreach { config =>
    logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} ${config.memoryLimit.toString}")(
      TransactionId.invokerWarmup)
    (1 to config.count).foreach { _ =>
      prewarmContainer(config.exec, config.memoryLimit)
    }
  }

  def logContainerStart(r: Run, containerState: String, activeActivations: Int, container: Option[Container]): Unit = {
    val namespaceName = r.msg.user.namespace.name
    val actionName = r.action.name.name
    val maxConcurrent = r.action.limits.concurrency.maxConcurrent
    val activationId = r.msg.activationId.toString
    r.adaptiveCpuShares = canUseCore //avs

    r.msg.transid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START(containerState),
      s"containerStart containerState: $containerState container: $container activations: $activeActivations of max $maxConcurrent action: $actionName namespace: $namespaceName activationId: $activationId and canUseCore: ${canUseCore} and r.adaptiveCpuShares ${r.adaptiveCpuShares}",
      akka.event.Logging.InfoLevel)
  }

  def receive: Receive = {
    // A job to run on a container
    //
    // Run messages are received either via the feed or from child containers which cannot process
    // their requests and send them back to the pool for rescheduling (this may happen if "docker" operations
    // fail for example, or a container has aged and was destroying itself when a new request was assigned)
    case r: Run =>
      // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
      val isResentFromBuffer = runBuffer.nonEmpty && runBuffer.dequeueOption.exists(_._1.msg == r.msg)

      // Only process request, if there are no other requests waiting for free slots, or if the current request is the
      // next request to process
      // It is guaranteed, that only the first message on the buffer is resent.
      if (runBuffer.isEmpty || isResentFromBuffer) {
        //logging.info(this,s"\t <adbg> <findCont> 1.0 activ: ${r.msg.activationId} searching for a warm container in busyPool: ${busyPool.size}")
        val createdContainer =
          // Is there enough space on the invoker for this action to be executed.
          if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {
            //logging.info(this,s"\t <adbg> <findCont> 1.1 activ: ${r.msg.activationId} I do have space in existing container in freePool: ${freePool.size}, trying to schedule in one of them..")
            // Schedule a job to a warm container
            ContainerPool
              .schedule(r.action, r.msg.user.namespace.name, freePool,logging)
              .map(container => (container, container._2.initingState)) //warmed, warming, and warmingCold always know their state
              .orElse(
                //logging.info("\t <adbg> <findCont> 1.2 Activ: ${r.msg.activationId} looks like freePool is not able to accommodate after all..")
                // There was no warm/warming/warmingCold container. Try to take a prewarm container or a cold container.
                // Is there enough space to create a new container or do other containers have to be removed?
                if (hasPoolSpaceFor(busyPool ++ freePool, r.action.limits.memory.megabytes.MB)) {                  
                  //logging.info(this,s"\t <adbg> <findCont> 1.3 activ: ${r.msg.activationId} can a prewarmed container accommodate me?")
                  // avs --begin
                  canUseCore = ((canUseCore+1)%4); 
                  cpuSharesPool.get(r.action.name.asString) match {
                    case Some(e) => cpuSharesPool(r.action.name.asString).dummyCall() // dummy operation
                    case None => 
                      //cpuSharesPool = cpuSharesPool + (r.action.name.asString -> MutableTriplet(0,0,r.msg.transid))
                      appIsoLatency.get(r.action.name.asString) match{
                        case Some(e) => 
                        var tempCpuShares = poolConfig.cpuShare(r.action.limits.memory.megabytes.MB) 
                        tempCpuShares = cpuSharesCheck(logging,tempCpuShares,1,r.action.name.asString)//,totalCpuShares)
                        case None => 
                          addFunctionRuntime(r.action.name.asString)
                      }

                      val myStandAloneRuntime = appIsoLatency(r.action.name.asString); // would have added it above, so it must be ok to access it here.
                      var curCpuShares = poolConfig.cpuShare(r.action.limits.memory.megabytes.MB) 
                      curCpuShares = cpuSharesCheck(logging,curCpuShares,1,r.action.name.asString)
                      cpuSharesPool = cpuSharesPool + (r.action.name.asString -> new TrackFunctionStats(r.action.name.asString,myStandAloneRuntime,r.action,curCpuShares,r.msg.transid,logging))                       
                  }
                  // avs --end

                  takePrewarmContainer(r.action)
                    .map(container => (container, "prewarmed"))
                    .orElse(Some(createContainer(r.action.limits.memory.megabytes.MB), "cold"))
                } else None)
              .orElse(
                //logging.info("\t <adbg> <findCont> 1.4 Activ: ${r.msg.activationId} trying to create a new container...")
                // Remove a container and create a new one for the given job
                ContainerPool
                // Only free up the amount, that is really needed to free up
                  .remove(freePool, Math.min(r.action.limits.memory.megabytes,memoryConsumptionOf(freePool)).MB) //avs
                  .map(removeContainer)
                  // If the list had at least one entry, enough containers were removed to start the new container. After
                  // removing the containers, we are not interested anymore in the containers that have been removed.
                  .headOption
                  .map(_ =>
                    takePrewarmContainer(r.action)
                      .map(container => (container, "recreatedPrewarm"))
                      .getOrElse(createContainer(r.action.limits.memory.megabytes.MB), "recreated")))

          } else None

        createdContainer match {
          case Some(((actor, data), containerState)) =>

            //logging.info(this,s"\t <adbg> <findCont> Anyway, for activ: ${r.msg.activationId} using a container in state: ${containerState} ")
            //increment active count before storing in pool map
            val newData = data.nextRun(r)
            val container = newData.getContainer

            // avs --begin
            var getAdaptiveCpuShares = 0
            cpuSharesPool.get(r.action.name.asString) match {
              case Some(e) => 
                //cpuSharesPool(r.action.name.asString).dummyCall() // dummy operation
                getAdaptiveCpuShares = cpuSharesPool(r.action.name.asString).getCurCpuShares()
              case None => 
                //cpuSharesPool = cpuSharesPool + (r.action.name.asString -> MutableTriplet(0,0,r.msg.transid))
                appIsoLatency.get(r.action.name.asString) match{
                  case Some(e) => 
                  var tempCpuShares = poolConfig.cpuShare(r.action.limits.memory.megabytes.MB) 
                  tempCpuShares = cpuSharesCheck(logging,tempCpuShares,1,r.action.name.asString)//,totalCpuShares)
                  case None => 
                    addFunctionRuntime(r.action.name.asString)
                }

                val myStandAloneRuntime = appIsoLatency(r.action.name.asString); // would have added it above, so it must be ok to access it here.
                var curCpuShares = poolConfig.cpuShare(r.action.limits.memory.megabytes.MB) 
                curCpuShares = cpuSharesCheck(logging,curCpuShares,1,r.action.name.asString)
                cpuSharesPool = cpuSharesPool + (r.action.name.asString -> new TrackFunctionStats(r.action.name.asString,myStandAloneRuntime,r.action,curCpuShares,r.msg.transid,logging))                 
                getAdaptiveCpuShares = cpuSharesPool(r.action.name.asString).getCurCpuShares()
            }

            r.adaptiveCpuShares = if(getAdaptiveCpuShares!=0) getAdaptiveCpuShares else poolConfig.cpuShare(r.action.limits.memory.megabytes.MB)
            // avs --end

            if (newData.activeActivationCount < 1) {
              logging.error(this, s"invalid activation count < 1 ${newData}")
            }

            //only move to busyPool if max reached
            if (!newData.hasCapacity()) {
              if (r.action.limits.concurrency.maxConcurrent > 1) {
                logging.info(
                  this,
                  s"container ${container} is now busy with ${newData.activeActivationCount} activations")
              }
              busyPool = busyPool + (actor -> newData)
              freePool = freePool - actor

            } else {
              //update freePool to track counts
              freePool = freePool + (actor -> newData)
            }
            // Remove the action that get's executed now from the buffer and execute the next one afterwards.
            if (isResentFromBuffer) {
              // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
              // from the buffer
              val (_, newBuffer) = runBuffer.dequeue
              runBuffer = newBuffer
              runBuffer.dequeueOption.foreach { case (run, _) => self ! run }
            }
            actor ! r // forwards the run request to the container
            logContainerStart(r, containerState, newData.activeActivationCount, container)
          case None =>
            // this can also happen if createContainer fails to start a new container, or
            // if a job is rescheduled but the container it was allocated to has not yet destroyed itself
            // (and a new container would over commit the pool)
            val isErrorLogged = r.retryLogDeadline.map(_.isOverdue).getOrElse(true)
            val retryLogDeadline = if (isErrorLogged) {
              logging.error(
                this,
                s"Rescheduling Run message, too many message in the pool, " +
                  s"freePoolSize: ${freePool.size} containers and ${memoryConsumptionOf(freePool)} MB, " +
                  s"busyPoolSize: ${busyPool.size} containers and ${memoryConsumptionOf(busyPool)} MB, " +
                  s"maxContainersMemory ${poolConfig.userMemory.toMB} MB, " +
                  s"userNamespace: ${r.msg.user.namespace.name}, action: ${r.action}, " +
                  s"needed memory: ${r.action.limits.memory.megabytes} MB, " +
                  s"waiting messages: ${runBuffer.size}")(r.msg.transid)
              Some(logMessageInterval.fromNow)
            } else {
              r.retryLogDeadline
            }
            if (!isResentFromBuffer) {
              // Add this request to the buffer, as it is not there yet.
              runBuffer = runBuffer.enqueue(r)
            }
          
            // avs --begin
            if(r.adaptiveCpuShares == -1){
              canUseCore = ((canUseCore+1)%4); //avs
              r.adaptiveCpuShares = canUseCore
            }
            // avs --end
            // As this request is the first one in the buffer, try again to execute it.
            self ! Run(r.action, r.msg, r.adaptiveCpuShares, retryLogDeadline)
        }
      } else {
        // There are currently actions waiting to be executed before this action gets executed.
        // These waiting actions were not able to free up enough memory.
        runBuffer = runBuffer.enqueue(r)
      }

    // Container is free to take more work
    case NeedWork(warmData: WarmedData) =>
      feed ! MessageFeed.Processed

      val oldData = freePool.get(sender()).getOrElse(busyPool(sender()))
      val newData = warmData.copy(activeActivationCount = oldData.activeActivationCount - 1)
      if (newData.activeActivationCount < 0) {
        logging.error(this, s"invalid activation count after warming < 1 ${newData}")
      }
      if (newData.hasCapacity()) {
        //logging.info(this, s"<adbg> <InNeedWork> 1.0 trackContId: ${trackContId} cont: ${warmData.container} adding it to freepool.. ")
        //remove from busy pool (may already not be there), put back into free pool (to update activation counts)
        freePool = freePool + (sender() -> newData)
        if (busyPool.contains(sender())) {
          //logging.info(this, s"<adbg> <InNeedWork> 1.1 trackContId: ${trackContId} is present in busyPool, so removing it..")
          busyPool = busyPool - sender()
          if (newData.action.limits.concurrency.maxConcurrent > 1) {
            //logging.info(this, s"<adbg> <InNeedWork> 1.2 trackContId: ${trackContId} maxConcurrent is >1")
            logging.info(
              this,
              s"concurrent container ${newData.container} is no longer busy with ${newData.activeActivationCount} activations")
          }
        }
      } else {
        //logging.info(this, s"<adbg> <InNeedWork> 2.0 trackContId: ${trackContId} doesn't have capacity. Moving it to busyPool from freepool.")
        busyPool = busyPool + (sender() -> newData)
        freePool = freePool - sender()
      }
    // Container is prewarmed and ready to take work
    case NeedWork(data: PreWarmedData) =>
      prewarmedPool = prewarmedPool + (sender() -> data)

    // Container got removed
    case ContainerRemoved =>
      // if container was in free pool, it may have been processing (but under capacity),
      // so there is capacity to accept another job request
      freePool.get(sender()).foreach { f =>
        freePool = freePool - sender()
        if (f.activeActivationCount > 0) {
          feed ! MessageFeed.Processed
        }
      }
      // container was busy (busy indicates at full capacity), so there is capacity to accept another job request
      busyPool.get(sender()).foreach { _ =>
        busyPool = busyPool - sender()
        feed ! MessageFeed.Processed
      }

    // avs --end

    // This message is received for one of these reasons:
    // 1. Container errored while resuming a warm container, could not process the job, and sent the job back
    // 2. The container aged, is destroying itself, and was assigned a job which it had to send back
    // 3. The container aged and is destroying itself
    // Update the free/busy lists but no message is sent to the feed since there is no change in capacity yet
    case RescheduleJob =>
      freePool = freePool - sender()
      busyPool = busyPool - sender()

    //avs --begin
    case UpdateStats(actionName: String,dummyReq: Int, initTime: Long,controllerID: ControllerInstanceId,runtime: Long,warmData: WarmedData) =>
      // WARNING: Pending, removing the member when container is removed.
      logging.info(this, s"<adbg> 1. UpdateStats for action ${actionName} runtime ${runtime} dummyReq: ${dummyReq} initTime: ${initTime} container: ${warmData.container} ");         
      var toUseCpuShares = 0
      cpuSharesPool.get(warmData.action.name.asString) match {
        case Some(curActStats) => 
          //logging.info(this, s"<adbg> <InNeedWork> actionName: ${warmData.action.name.asString} is present in cpuSharesPool and a new container is being added to it. ")
          //curActStats.addContainer(warmData.container)  
          //if(dummyReq==0) 
          trackContId = curActStats.addContainer(warmData.container,trackContId,dummyReq) // will update contId, if 
          //else logging.info(this, s"<adbg> <US:AC1> this was a dummy request, not doing anything with it..")            
          //logging.info(this, s"<adbg> <US:AC1> actionName: ${warmData.action.name.asString} container ${warmData.container} is present in cpuSharesPool and container (trackContId: ${trackContId-1} and cpuShares: ${toUseCpuShares}) being updated to it.!")
        case None => 
          logging.info(this, s"<adbg> <US:AC2> actionName: ${warmData.action.name.asString} container ${warmData.container} is NOT present in cpuSharesPool and a new container is NOT being added to it. HANDLE it!")
      }
      
      // only tracking non-cold starts..
      if(initTime==0) {
        cpuSharesPool.get(actionName) match {
          case Some(curActTrackedStats) => 
            //avgActionRuntime(actionName).addRuntime(runtime)         
            curActTrackedStats.addRuntime(runtime)         
          case None => 
            //avgActionRuntime = avgActionRuntime + (actionName -> MutableTriplet(runtime,1,))
            logging.info(this, s"<adbg> 2. UpdateStats for action ${actionName} and the runtime is ${runtime} is not updated, because the triplet with transid wasn't created properly, HANDLE it!");         
        }     
      }
      // But will send completion-ack to stats tracking in LB.
      // Not sure whether I want to always send it back, for now, assuming that this is the design we will stick with.

      //var curActStats : toRelayActionStats = getCurActionStats(actionName,logging)
      var numConts = getCurActionConts(actionName,logging)
      var numWindowConts = getCurActionWindowConts(actionName,logging)
      if(dummyReq>0){
        logging.info(this, s"<adbg> 2.1 Apparently this was a dummyReq: ${dummyReq} ");         
        relayActionStats(actionName,runtime,500*1000,numConts,numWindowConts,controllerID.asString)
      }
      else
        relayActionStats(actionName,runtime,initTime,numConts,numWindowConts,controllerID.asString)

      logging.info(this, s"<adbg> <UpdateStats> end-getCurActionStats ")

    case RemoveContTracking(container: Container, actionName: String) => 
      cpuSharesPool.get(actionName) match {
        case Some(curActTrackedStats) => 
          curActTrackedStats.removeContainer(container)
        case None =>                    
          logging.info(this, s"<adbg> <RemoveContTracking> actionName: ${actionName} was NOT present in cpuSharesPool and hence nothing is being done, HANDLE it! ")

      }         

    case getAllLatency(curActName: String,controllerID: Int) =>
      logging.info(this, s"<adbg> <getAllLatency> start-getCurActionStats ")
      var curActStats : toRelayActionStats = getCurActionStats(curActName,logging)
      // new toRelayActionStats(curActName, tempAvgLatency, tempNumConts)
      // toRelayActionStats(val actionName: String,val avgLatency: Long,val numConts: Int)
      relayActionStats(curActStats.actionName,curActStats.avgLatency,2,curActStats.numConts,curActStats.numWindowConts,controllerID.toString)
      logging.info(this, s"<adbg> <getAllLatency> end-getCurActionStats ")

    //avs --end
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(memoryLimit: ByteSize): (ActorRef, ContainerData) = {
    val ref = childFactory(context)

    val data = MemoryData(memoryLimit)
    freePool = freePool + (ref -> data)
    ref -> data
    
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize): Unit =
    childFactory(context) ! Start(exec, memoryLimit)

  /**
   * Takes a prewarm container out of the prewarmed pool
   * iff a container with a matching kind and memory is found.
   *
   * @param action the action that holds the kind and the required memory.
   * @return the container iff found
   */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ActorRef, ContainerData)] = {
    val kind = action.exec.kind
    val memory = action.limits.memory.megabytes.MB
    prewarmedPool
      .find {
        case (_, PreWarmedData(_, `kind`, `memory`, _)) => true
        case _                                          => false
      }
      .map {
        case (ref, data) =>
          // Move the container to the usual pool
          freePool = freePool + (ref -> data)
          prewarmedPool = prewarmedPool - ref
          // Create a new prewarm container
          // NOTE: prewarming ignores the action code in exec, but this is dangerous as the field is accessible to the
          // factory
          prewarmContainer(action.exec, memory)
          (ref, data)
      }
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: ActorRef) = {
    toDelete ! Remove
    freePool = freePool - toDelete
    busyPool = busyPool - toDelete
  }

  /**
   * Calculate if there is enough free memory within a given pool.
   *
   * @param pool The pool, that has to be checked, if there is enough free memory.
   * @param memory The amount of memory to check.
   * @return true, if there is enough space for the given amount of memory.
   */
  def hasPoolSpaceFor[A](pool: Map[A, ContainerData], memory: ByteSize): Boolean = {
    val cur_poolMemConsumption = memoryConsumptionOf(pool)
    logging.info(this, s"<adbg> Checking for pool space -- i.e. (${cur_poolMemConsumption} + ${memory.toMB}) <= (${poolConfig.userMemory.toMB})") //avs
    memoryConsumptionOf(pool) + memory.toMB <= poolConfig.userMemory.toMB
  }

}

object ContainerPool {

  //protected[containerpool]  var cpuSharesPool = immutable.Map.empty[ActorRef, funcConfigTracking] //avs
  protected[containerpool]  var cpuSharesPool = immutable.Map.empty[String, TrackFunctionStats] //avs
  protected[containerpool] var totalCpuShares = 4*1024 //1024; // WARNING: Should move this to poolConfig and to make it inferrable.
  protected[containerpool] var reductThreshold: Double = 0.0 // 0.0
  /**
   * Calculate the memory of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The memory consumption of all containers in the pool in Megabytes.
   */
  protected[containerpool] def memoryConsumptionOf[A](pool: Map[A, ContainerData]): Long = {
    pool.map(_._2.memoryLimit.toMB).sum
  }
  // avs --begin

  protected[containerpool] var appIsoLatency = immutable.Map.empty[String,Long] 
  var appClass = immutable.Map.empty[String,String] 
    // WoSC apps
   appIsoLatency = appIsoLatency + ("imageResizing_v1"->660) ; appClass = appClass + ("imageResizing_v1" -> "ET")
    appIsoLatency = appIsoLatency + ("servingCNN_v1"->1800) ; appClass = appClass + ("servingCNN_v1" -> "ET")
    appIsoLatency = appIsoLatency + ("realTimeAnalytics_v1"->750) ; appClass = appClass + ("realTimeAnalytics_v1"->"ET")
    appIsoLatency = appIsoLatency + ("rodinia_nn_v1"->7240); appClass = appClass + ("rodinia_nn_v1"->"MP")
    appIsoLatency = appIsoLatency + ("euler3d_cpu_v1"->19630) ; appClass = appClass + ("realTimeAnalytics_v1"->"MP")
    appIsoLatency = appIsoLatency + ("invokerHealthTestAction0"->0) ; appClass = appClass + ("realTimeAnalytics_v1"->"MP")

    // Test Set MP apps -- 
    /* Compas cloud
    appIsoLatency = appIsoLatency + ("matmulaction_v1" -> 35670); appClass = appClass + ("matmulaction_v1" -> "MP")
    appIsoLatency = appIsoLatency + ("matmulaction_v2" -> 21000); appClass = appClass + ("matmulaction_v2" -> "MP")
    appIsoLatency = appIsoLatency + ("matadd_v1" -> 300000); appClass = appClass + ("matadd_v1" -> "MP");
    appIsoLatency = appIsoLatency + ("sort_v1" -> 40375); appClass = appClass + ("sort_v1" -> "MP")

    // Test Set ET apps -- 
    appIsoLatency = appIsoLatency + ("emailGen_v1" -> 830);      appClass = appClass + ("emailGen_v1"->"ET")
    appIsoLatency = appIsoLatency + ("stockAnalysis_v1" -> 710);      appClass = appClass + ("stockAnalysis_v1"->"ET")
    appIsoLatency = appIsoLatency + ("fileEncrypt_v1" -> 760);      appClass = appClass + ("fileEncrypt_v1"->"ET")
    appIsoLatency = appIsoLatency + ("serving_lrReview_v1" -> 890);      appClass = appClass + ("serving_lrReview_v1"->"ET")
    */

    appIsoLatency = appIsoLatency + ("matmulaction_v1" -> 20500); appClass = appClass + ("matmulaction_v1" -> "MP")
    appIsoLatency = appIsoLatency + ("matmulaction_v2" -> 20500); appClass = appClass + ("matmulaction_v2" -> "MP")
    appIsoLatency = appIsoLatency + ("matadd_v1" -> 300000); appClass = appClass + ("matadd_v1" -> "MP");
    appIsoLatency = appIsoLatency + ("sort_v1" -> 50800); appClass = appClass + ("sort_v1" -> "MP")

    // Test Set ET apps -- 
    appIsoLatency = appIsoLatency + ("emailGen_v1" -> 710);      appClass = appClass + ("emailGen_v1"->"ET")
    appIsoLatency = appIsoLatency + ("stockAnalysis_v1" -> 808);      appClass = appClass + ("stockAnalysis_v1"->"ET")
    appIsoLatency = appIsoLatency + ("fileEncrypt_v1" -> 882);      appClass = appClass + ("fileEncrypt_v1"->"ET")
    appIsoLatency = appIsoLatency + ("serving_lrReview_v1" -> 1080);      appClass = appClass + ("serving_lrReview_v1"->"ET")

    appIsoLatency = appIsoLatency + ("fe_v1" -> 850);      appClass = appClass + ("fe_v1"->"ET")
    appIsoLatency = appIsoLatency + ("fe_v2" -> 1420);      appClass = appClass + ("fe_v2"->"ET")
    appIsoLatency = appIsoLatency + ("fe_v3" -> 2500);      appClass = appClass + ("fe_v3"->"ET")

  def getFunctionRuntime(functionName: String): Long = {

    appIsoLatency.get(functionName) match {
      case Some(funcStandaloneRuntime) => 
      funcStandaloneRuntime
    case None =>
      var maxRuntime:Long = 60*5*1000
      maxRuntime
    }
    
  }

  protected[containerpool] def getActionType(functionName: String): String = {
    appClass.get(functionName) match {
      case Some(funcClass) => 
      funcClass
    case None =>
      "MP"
    }    
  }
 
  protected[containerpool] def cpuSharesCheck[A] = (logging: AkkaLogging,toUpdateCpuShares: Int,numContsToUpdate:Int,toIncActionName:String)=> { 
  var pool: Map[String,TrackFunctionStats] = cpuSharesPool;
  var cur_poolCpuSharesConsumption = cpuSharesConsumptionOf(pool)
  var resUpdatedShares = toUpdateCpuShares 

  var canUpdate: Boolean = ( cpuSharesConsumptionOf(pool) + (numContsToUpdate  * resUpdatedShares) ) <= totalCpuShares    
  logging.info(this, s"<adbg><cpuSharesCheck> 0. Checking for getCpuSharesFor -- toIncActionName: ${toIncActionName} i.e. (${cur_poolCpuSharesConsumption} + ${numContsToUpdate * resUpdatedShares}) <= (${totalCpuShares})") 
  if(canUpdate){
    logging.info(this, s"<adbg><cpuSharesCheck> CAN-UPDATE!!") 
    resUpdatedShares
  }else{

    var befUpdatingAccumShares = 0
    var avgCpuSharesReduction = 0
    var diffCpuShares = 0
    var poolSize = pool.size
    var numIters = 0
    var numOtherContainers = 0 
    var decAll: Boolean = false
    var sumOfAllCpuShares = 0

    pool.keys.foreach{ curActName =>
      var myTrackedStats: TrackFunctionStats = pool(curActName)
      if(myTrackedStats.actionName != toIncActionName){
        numOtherContainers = numOtherContainers + myTrackedStats.numContainerTracked()
        logging.info(this, s"<adbg><cpuSharesCheck> action: ${myTrackedStats.actionName} and I have ${myTrackedStats.numContainerTracked()} containers. numOtherContainers: ${numOtherContainers}") 
      }
    }      

    while(!canUpdate){
      
      befUpdatingAccumShares = cpuSharesConsumptionOf(pool)
      diffCpuShares = ( (numContsToUpdate  * resUpdatedShares) + cpuSharesConsumptionOf(pool) ) - totalCpuShares;
      //numOtherContainers = (pool.size - numContsToUpdate)
      if(numOtherContainers>0) avgCpuSharesReduction = diffCpuShares/numOtherContainers

      logging.info(this, s"<adbg><cpuSharesCheck> numIters: ${numIters} canUpdate: ${canUpdate} So, will rebalance cpuShares once. numOtherContainers: ${numOtherContainers} pool-cpu-shares: ${cpuSharesConsumptionOf(pool)} numContsToUpdate: ${numContsToUpdate} resUpdatedShares: ${resUpdatedShares} avgCpuSharesReduction: ${avgCpuSharesReduction} totalCpuShares: ${totalCpuShares}") 
      
      //rebalanceCpuShares(pool,avgCpuSharesReduction,toIncActionName,logging)
      if(avgCpuSharesReduction>=0)
        rebalanceCpuShares(avgCpuSharesReduction,toIncActionName,decAll,logging)

      sumOfAllCpuShares = cpuSharesConsumptionOf(pool)
      canUpdate = ( sumOfAllCpuShares + (numContsToUpdate  * resUpdatedShares) ) <= totalCpuShares 
      if(!canUpdate){

        canUpdate = true
        diffCpuShares = ( (numContsToUpdate  * resUpdatedShares) + sumOfAllCpuShares  ) - totalCpuShares;

        if(numContsToUpdate!=0) avgCpuSharesReduction = diffCpuShares/numContsToUpdate
        resUpdatedShares = resUpdatedShares - avgCpuSharesReduction

        if(resUpdatedShares<0){
          printAllCpuShares(logging)
          sumOfAllCpuShares = cpuSharesConsumptionOf(pool) 
          resUpdatedShares = 0
        }
        //logging.info(this, s"<adbg><cpuSharesCheck> 3. canUpdate: ${canUpdate} avgCpuSharesReduction: ${avgCpuSharesReduction} befUpdatingAccumShares: ${befUpdatingAccumShares} afterUpdatingAccumShares: ${afterUpdatingAccumShares} diffCpuShares: ${diffCpuShares}") 
      }
      numIters+=1
    }
    
    logging.info(this, s"<adbg><cpuSharesCheck> Done rebalancing cpuShares pool-cpu-shares: ${cpuSharesConsumptionOf(pool)}, resUpdatedShares: ${resUpdatedShares} totalCpuShares: ${totalCpuShares}")       
    resUpdatedShares  
  }    
  }

  def printAllCpuShares(logging: AkkaLogging): Unit = {
    var pool: Map[String,TrackFunctionStats] = cpuSharesPool;
    var tempCpuShares = 0; var sumOfAllCpuShares = 0; var totNumConts = 0
    var cpuSharesList = new mutable.ListBuffer[Int]
    var actionNumConts = new mutable.ListBuffer[Int]
    var numActions = 0; var decAll: Boolean = false

    logging.info(this, s"<adbg><printAllCpuShares> BEGIN *************** ")      
    //private var myContainers = mutable.Map.empty[Container, Int]
    pool.keys.foreach{ curActName =>
      var myTrackedStats: TrackFunctionStats = pool(curActName)
      myTrackedStats.printAllContainers()
      totNumConts = totNumConts + myTrackedStats.numContainerTracked()
      tempCpuShares = myTrackedStats.accumAllCpuShares()
      sumOfAllCpuShares = sumOfAllCpuShares + tempCpuShares
      cpuSharesList+=tempCpuShares
      actionNumConts+=myTrackedStats.numContainerTracked()
      numActions+=1
    }

    logging.info(this, s"<adbg><printAllCpuShares> End: ${sumOfAllCpuShares} *************** ")  
    // so, sumOfAllCpuShares is greater than totalCpuShares. Should reduce it..
    if( (sumOfAllCpuShares> totalCpuShares) && (totNumConts>0)){
      //equitableRebalance(sumOfAllCpuShares,totNumConts)
      var diffCpuShares = (sumOfAllCpuShares - totalCpuShares); // / ()
      var avgCpuSharesReduction: Int = diffCpuShares/totNumConts; var percentReduct: Double = 0
      if(avgCpuSharesReduction >= 4){
        var numActionsAffected = 0 // if some action has to give up more than its' reductThreshold, we will cap everyone at reductThreshold
        logging.info(this, s"<adbg><RebalWhilePrint> sumOfAllCpuShares: ${sumOfAllCpuShares} totalCpuShares: ${totalCpuShares}  diffCpuShares: ${diffCpuShares} totNumConts: ${totNumConts} avgCpuSharesReduction: ${avgCpuSharesReduction}")  
        // Figure out how much am I impacting..
        var curCpuShare = 0; var numConts = 0; var idx = 0; var tempCalc: Double = 0
        for(idx <- 0 until numActions){
          curCpuShare = cpuSharesList(idx)
          numConts = actionNumConts(idx)
          tempCalc =  (curCpuShare - (numConts*avgCpuSharesReduction))
          percentReduct = tempCalc.toDouble/curCpuShare
          if(percentReduct < reductThreshold){
            numActionsAffected+=1
          }
          logging.info(this, s"<adbg><RebalWhilePrint> idx: ${idx} numActionsAffected: ${numActionsAffected} avgCpuSharesReduction: ${avgCpuSharesReduction} curCpuShare: ${curCpuShare} numConts: ${numConts} percentReduct: ${percentReduct} ")  
        }
        if(numActionsAffected==0){
          // ok, all of them will shed less than reductThreshold, so go ahead and reduce it.
          decAll = false
          rebalanceCpuShares(avgCpuSharesReduction,"invokerHealthTestAction0",decAll,logging)
        }else{
          // ok, not all of them will shed less than reductThreshold, so cap it at reductThreshold
          decAll = true
          rebalanceCpuShares(avgCpuSharesReduction,"invokerHealthTestAction0",decAll,logging)
        }

      }
    }

  }

  def rebalanceCpuShares[A](suggesstedSharesReduction: Int,toIncActionName: String,decAll:Boolean,logging: AkkaLogging): Unit = {
    var pool: Map[String,TrackFunctionStats] = cpuSharesPool;
    var avgCpuSharesReduction = suggesstedSharesReduction
    pool.keys.foreach{ curActionName => 
      var myTrackedStats: TrackFunctionStats = pool(curActionName)
      
      if(myTrackedStats.actionName != toIncActionName){
        var updatedCpuShares = 0; 
        var myCurShares = 0; var calcCpuShares: Double = 0

        myTrackedStats.myContainers.keys.foreach{ curCont => 
          myCurShares = myTrackedStats.getCurContCpuShares(curCont)
          
          if(decAll){
            // ok, it's coming from printAllCpuShares and some of them have to give up more than reductThreshold, so cap it!
            calcCpuShares = (1-reductThreshold)*myCurShares 
            avgCpuSharesReduction = ( calcCpuShares.toInt -1) // -1 just to ensure updatedCpuShares goes through well! // should ensure reductThreshold<=1
            logging.info(this, s"<adbg><rebalanceCpuShares> ActName: ${myTrackedStats.actionName} myCurShares: ${myCurShares} calcCpuShares: ${calcCpuShares} avgCpuSharesReduction: ${avgCpuSharesReduction} reductThreshold: ${reductThreshold}")                    
          } 

          updatedCpuShares = myCurShares - avgCpuSharesReduction

          if((updatedCpuShares >= myTrackedStats.getDefaultCpuShares()) && (updatedCpuShares >= reductThreshold*myCurShares)){
            logging.info(this, s"<adbg><rebalanceCpuShares> Going to update my CPUSHARES. actName: ${myTrackedStats.actionName} my id: ${myTrackedStats.getCurContID(curCont)} and my cpuShares is ${myTrackedStats.getCurContCpuShares(curCont)} and updatedCpuShares: ${updatedCpuShares}")                    
            curCont.updateCpuShares(myTrackedStats.getCurTxnId(),updatedCpuShares)
            myTrackedStats.setCurContCpuShares(curCont,updatedCpuShares)            
          }else{
            logging.info(this, s"<adbg><rebalanceCpuShares> NOTT going to update my cpushares. actName: ${myTrackedStats.actionName} my id: ${myTrackedStats.getCurContID(curCont)} and my cpuShares is ${myTrackedStats.getCurContCpuShares(curCont)} and updatedCpuShares: ${updatedCpuShares} and reductThreshold: ${reductThreshold}")                    
          }
        }
      }
    }
  }

  def getCurActionConts(curActName: String, logging: AkkaLogging): Int = {
    var pool: Map[String,TrackFunctionStats] = cpuSharesPool;

    pool.get(curActName) match {
      case Some(myTrackedStats) =>
        logging.info(this, s"<adbg><getCurActionConts> <initData> action: ${curActName} tempNumConts: ${myTrackedStats.numContainerTracked()}")      
        myTrackedStats.numContainerTracked()
      case None =>
        0
    }
  }  

  def getCurActionWindowConts(curActName: String, logging: AkkaLogging): Int = {
    var pool: Map[String,TrackFunctionStats] = cpuSharesPool;

    pool.get(curActName) match {
      case Some(myTrackedStats) =>
        logging.info(this, s"<adbg><getCurActionWindowConts> <initData> action: ${curActName} tempNumConts: ${myTrackedStats.getCurWindowNumConts()}")      
        myTrackedStats.getCurWindowNumConts()
      case None =>
        0
    }
  }

  def getCurActionStats(curActName: String, logging: AkkaLogging): toRelayActionStats = {
    var pool: Map[String,TrackFunctionStats] = cpuSharesPool;
    pool.get(curActName) match {
      case Some(myTrackedStats) =>
        var tempAvgLatency = myTrackedStats.getAvgRuntime()
        var tempNumConts = myTrackedStats.numContainerTracked()
        var tempCurWindowNumConts = myTrackedStats.getCurWindowNumConts()
        logging.info(this, s"<adbg><getCurActionStats> <initData> action: ${curActName} tempAvgLatency: ${tempAvgLatency} tempNumConts: ${tempNumConts}")      
        new toRelayActionStats(curActName, tempAvgLatency, tempNumConts,tempCurWindowNumConts)
      case None =>
        new toRelayActionStats(curActName, 0, 0,0)
    }
  }  

  /**
   * Calculate the cpuShares of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The cpuShares of all containers in the pool
   */
  //protected[containerpool] def cpuSharesConsumptionOf[A](pool: Map[A, Int]): Int = {
  /*protected[containerpool] def cpuSharesConsumptionOf[A](pool: Map[A, funcConfigTracking]): Int = {
    pool.map(_._2.getCurContCpuShares()).sum
  } */ 
  protected[containerpool] def cpuSharesConsumptionOf[A](pool: Map[A, TrackFunctionStats]): Int = {
    var tempCpuShares = 0
    pool.keys.foreach{ curActionName =>
      tempCpuShares = tempCpuShares + pool(curActionName).accumAllCpuShares()
    }
    tempCpuShares
  } 
  // avs --end

  /**
   * Finds the best container for a given job to run on.
   *
   * Selects an arbitrary warm container from the passed pool of idle containers
   * that matches the action and the invocation namespace. The implementation uses
   * matching such that structural equality of action and the invocation namespace
   * is required.
   * Returns None iff no matching container is in the idle pool.
   * Does not consider pre-warmed containers.
   *
   * @param action the action to run
   * @param invocationNamespace the namespace, that wants to run the action
   * @param idles a map of idle containers, awaiting work
   * @return a container if one found
   */
  protected[containerpool] def schedule[A](action: ExecutableWhiskAction,
                                           invocationNamespace: EntityName,
                                           idles: Map[A, ContainerData],logging: AkkaLogging): Option[(A, ContainerData)] = {

    idles.find {
        case (_, c @ WarmedData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true

        case (a,curCont) =>
          curCont.getContainer match {
            case Some(container) =>
              if(curCont.initingState=="warmed"){
                val castedContData = curCont.asInstanceOf[WarmedData]
                val actionName = castedContData.action.name.name
                // Fix-it: Only checking for action-name, should also incorporate version check. 
                if( (actionName == action.name.name) && castedContData.hasCapacity() ){
                  logging.info(this,s"<avs> <findCont:schedule> 1.5-P1-0 cont: ${container} castedContData.invocName: ${castedContData.invocationNamespace} actionName: ${actionName} invocationNamespace: ${`invocationNamespace`} lastUsed: ${curCont.lastUsed} initState: ${curCont.initingState} hasCapacity: ${castedContData.hasCapacity()}")  
                  true
                }else{
                  logging.info(this,s"<avs> <findCont:schedule> 1.5-P1-1 cont: ${container} castedContData.invocName: ${castedContData.invocationNamespace} actionName: ${actionName} invocationNamespace: ${`invocationNamespace`} lastUsed: ${curCont.lastUsed} initState: ${curCont.initingState} hasCapacity: ${castedContData.hasCapacity()}")
                  false
                }
                
              }else{
                logging.info(this,s"<avs> <findCont:schedule> 1.5-P2. cont: ${container} invocationNamespace: ${`invocationNamespace`} lastUsed: ${curCont.lastUsed} initState: ${curCont.initingState} hasCapacity: ${curCont.hasCapacity()}")  
                false
              }
              //false
            case None =>
              logging.info(this,s"<avs> <findCont:schedule> 1.5-P3. cont: didnt-find lastUsed: ${curCont.lastUsed} initState: ${curCont.initingState} hasCapacity: ${curCont.hasCapacity()}")
              false
          }

        case _ => false
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                 => false
        }
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingColdData(`invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true           
          case _                                                                                  => false       
      }
    }

    /*idles
      .find {
        case (_, c @ WarmedData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
        case _                                                                                => false
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                 => false
        }
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingColdData(`invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                  => false
        }
      }
    */
  }

  /**
   * Finds the oldest previously used container to remove to make space for the job passed to run.
   * Depending on the space that has to be allocated, several containers might be removed.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   *
   * @param pool a map of all free containers in the pool
   * @param memory the amount of memory that has to be freed up
   * @return a list of containers to be removed iff found
   */
  @tailrec
  protected[containerpool] def remove[A](pool: Map[A, ContainerData],
                                         memory: ByteSize,
                                         toRemove: List[A] = List.empty): List[A] = {
    // Try to find a Free container that does NOT have any active activations AND is initialized with any OTHER action
    val freeContainers = pool.collect {
      // Only warm containers will be removed. Prewarmed containers will stay always.
      case (ref, w: WarmedData) if w.activeActivationCount == 0 =>
        ref -> w
    }

    if (memory > 0.B && freeContainers.nonEmpty && memoryConsumptionOf(freeContainers) >= memory.toMB) {
      // Remove the oldest container if:
      // - there is more memory required
      // - there are still containers that can be removed
      // - there are enough free containers that can be removed
      val (ref, data) = freeContainers.maxBy(_._2.lastUsed)
      // Catch exception if remaining memory will be negative
      val remainingMemory = Try(memory - data.memoryLimit).getOrElse(0.B)
      remove(freeContainers - ref, remainingMemory,toRemove ++ List(ref))
    } else {
      // If this is the first call: All containers are in use currently, or there is more memory needed than
      // containers can be removed.
      // Or, if this is one of the recursions: Enough containers are found to get the memory, that is
      // necessary. -> Abort recursion
      toRemove
    }
  }

  def props(factory: ActorRefFactory => ActorRef,
            poolConfig: ContainerPoolConfig,
            feed: ActorRef,
            prewarmConfig: List[PrewarmingConfig] = List.empty,
            relayActionStats: ActiveLoadResp
          ) =
    Props(new ContainerPool(factory, feed, prewarmConfig, poolConfig,relayActionStats))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(count: Int, exec: CodeExec[_], memoryLimit: ByteSize)
