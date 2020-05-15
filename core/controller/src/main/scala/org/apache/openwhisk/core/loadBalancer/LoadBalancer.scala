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

import scala.concurrent.Future
import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.spi.Spi
import scala.concurrent.duration._
import scala.collection.mutable //avs
import scala.collection.immutable //avs
import scala.collection.mutable.ListBuffer //avs
import java.time.Instant // avs
import scala.collection.immutable.ListMap // avs
import scala.math // avs
//import util.control.Breaks._ // avs
// avs --begin

class functionInfo {
  // Adding cont Runtime
  var appIsoLatency = immutable.Map.empty[String,Long] 
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

  def getActionType(functionName: String): String = {
    appClass.get(functionName) match {
      case Some(funcClass) => 
      funcClass
    case None =>
      "MP"
    }    
  }

  var latencyTolerance = 1.15
  var safeBeginThreshold:Double = 0.0
  var safeEndThreshold:Double = 0.25
  var spawnInNewInvokerBeginThd: Double = safeEndThreshold
  var spawnInNewInvokerEndThd: Double = 0.5
  var warnBeginThreshold:Double = spawnInNewInvokerEndThd
  var warnEndThreshold:Double = 0.75
  var unsafeBeginThreshold:Double = warnEndThreshold
  var unsafeEndThreshold:Double = 100000.0 

  var opZoneUndetermined = 0
  var opZoneSafe = 1
  var opZoneSpawnInNewInvoker = 2
  var opZoneWarn = 3
  var opZoneUnSafe = 4
  var startingOpZone = opZoneUndetermined

  var statsTimeoutInMilli: Long = 60*1000 // 1 minute is the time for docker to die. So, the stats are going to be outdate.
  var heartbeatTimeoutInMilli: Long = 20*1000
  var resetNumInstances = 2 
  var minResetTimeInMilli = 1*1000
  var movWindow_numReqs_ET = 5
  var movWindow_numReqs_MP = 1

  // square root based proactive spawning..
  var minProactivWaitTime: Long = 5*1000 // minimum wait before issuing next set of proactive spawning requests. // TODO: Should make this a install time parameter, so that both controller and invoker are in sync?
  var minZoneBaseProactivWaitTime: Long = 20*1000
  var zoneBasedProactivReqs = 2

  //Sched related params
  var schedDefault: Int = 0
  var schedRR: Int = 1
  var rrThd: Double = spawnInNewInvokerEndThd // should be set based on zones

  val latWindowSize = 20 // only for testing, during eval-run, should be at least 20.
  val prevUsedThreshold = 2 // N/2 for now..
  val minNonWarnRatio = 0.5 // minimum number of non-warning invokers to have..
  // removed var lastUsed: Long, as the second param
}

// the stats of an action in a given invoker
class ActionStatsPerInvoker(val actionName: String,val myInvokerID: Int,logging: Logging) extends functionInfo{
  val standaloneRuntime: Long = getFunctionRuntime(actionName)
  val statsResetTimeout: Long = if( (resetNumInstances * standaloneRuntime) > minResetTimeInMilli) resetNumInstances * standaloneRuntime else minResetTimeInMilli
  var numConts = 0
  var numWindowConts = 0
  var movingAvgLatency: Long = 0 
  var cumulSum: Long = 0
  var runningCount: Long = 0
  var actionType: String  = getActionType(actionName) // ET or MessagingProvider
  var curAct_movWindow_numReqs = 0
  if(actionType == "ET")
    curAct_movWindow_numReqs = movWindow_numReqs_ET
  else
    curAct_movWindow_numReqs = movWindow_numReqs_MP

  var opZone = startingOpZone //opZoneSafe // 0: safe ( 0 to 50% of latency); 1: will reach un-safe soon, 2: unsafe
  var lastUpdated: Long = Instant.now.toEpochMilli

  var latWindow: ListBuffer[Long] = ListBuffer.fill(latWindowSize)(0) //var latWindow: ListBuffer[Long] = new mutable.ListBuffer[Long]
  var latWindowIdx: Int = 0

  // FIX-IT: should be more accurate (based on when responses come?) instead of estimates..
  var proactiveTimeoutInMilli: Long =  statsTimeoutInMilli + (10*1000) + (1*standaloneRuntime) 
  
  def simplePrint(toPrintAction:String, toPrintLatency: Long, toPrintNumConts:Int): Unit = {
   logging.info(this,s"\t <adbg> <simplePrint> <ASPI> action: ${toPrintAction} has averageLatency: ${toPrintLatency} and #conts: ${toPrintNumConts}") 
  }

  def opZoneUpdate(): Unit = {
    var toSetOpZone = opZone
    var latencyRatio: Double = (movingAvgLatency.toDouble/standaloneRuntime)
    var toleranceRatio: Double = if(latencyRatio > 1.0) ((latencyRatio-1)/(latencyTolerance-1)) else safeBeginThreshold
    logging.info(this,s"\t <adbg> <ASPI:opZoneUpdate> action: ${actionName} and curOpZone is ${opZone} movingAvgLatency: ${movingAvgLatency} and standaloneRuntime: ${standaloneRuntime} and latencyRatio: ${latencyRatio} and toleranceRatio: ${toleranceRatio}") 

    if( (toleranceRatio >= safeBeginThreshold) && (toleranceRatio <= safeEndThreshold)){
        opZone = opZoneSafe
        logging.info(this,s"\t <adbg> <ASPI:opZoneUpdate> action:${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${safeBeginThreshold} and end: ${safeEndThreshold}, so opzone is SAFE --${opZone}.") 
    }else if( (toleranceRatio >= spawnInNewInvokerBeginThd) && (toleranceRatio <= spawnInNewInvokerEndThd)){
        opZone = opZoneSpawnInNewInvoker
        logging.info(this,s"\t <adbg> <ASPI:opZoneUpdate> action:${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${safeBeginThreshold} and end: ${safeEndThreshold}, so opzone is SAFE --${opZone}.")       
    }
    else if( (toleranceRatio >= warnBeginThreshold) && (toleranceRatio <= warnEndThreshold)){
      opZone = opZoneWarn
      logging.info(this,s"\t <adbg> <ASPI:opZoneUpdate> action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${warnBeginThreshold} and end: ${warnEndThreshold}, so opzone is in WARNING safe --${opZone}") 
    }else if( (toleranceRatio >= unsafeBeginThreshold) && (toleranceRatio <= unsafeEndThreshold)){
      opZone = opZoneUnSafe
      logging.info(this,s"\t <adbg> <ASPI:opZoneUpdate> action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${unsafeBeginThreshold} and end: ${unsafeEndThreshold}, so opzone is UNSAFE --${opZone}") 
    }else{
      opZone = opZoneUnSafe
      logging.info(this,s"\t <adbg> <ASPI:opZoneUpdate> action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently in a weird region, so it should be declared UNSAFE --${opZone}") 
    }
  }

  def resetStats(curTime: Long,statsResetFlag: Boolean): Unit = {
    // Assuming that, if either it is not updated in the past, 
    // a. statsTimeoutInMilli : Atleast a container would have died.
    // b. statsResetTimeout: It would have passed some time, so, the load should have subsided..

    // EXPT-WARNING: Might be better to issue a load request and refresh stats!, instead of resetting willy nilly!
    if(statsResetFlag && opZone != opZoneUnSafe){
      logging.info(this,s"\t <adbg> <ASPI:resetStats> action: ${actionName}, myInvokerID: ${myInvokerID} has passed statsResetFlag: ${statsResetFlag} but opZone is ${opZone} not unsafe, not doing anything..")       
    }else{      
      val prevOpZone = opZone
      if(statsResetFlag && opZone == opZoneUnSafe){
        numConts = 1
        opZone = opZoneWarn
      }else{
        numConts = 0
        opZone = startingOpZone
      }

      movingAvgLatency = 0
      cumulSum = 0
      runningCount = 0
    
      latWindow = ListBuffer.fill(latWindowSize)(0)
      latWindowIdx = 0 

      lastUpdated = Instant.now.toEpochMilli        
      logging.info(this,s"\t <adbg> <ASPI:resetStats> action: ${actionName}, myInvokerID: ${myInvokerID} resetting my stats at lastUpdated: ${lastUpdated} and statsResetFlag: ${statsResetFlag} and prevOpZone: ${prevOpZone}") 
      
    }
    
  }

  // update(latencyVal,initTime,toUpdateNumConts)
  def update(latency: Long, initTime: Long, toUpdateNumConts: Int, toUpdateNumWindowConts: Int): Unit = {
    if(initTime==0){ // warm starts only!!
      latWindow(latWindowIdx) = latency
      latWindowIdx = (latWindowIdx+1)%latWindowSize
      cumulSum = latWindow.reduce((x,y)=> x+y )  
      runningCount+= 1
      lastUpdated = Instant.now.toEpochMilli      
      movingAvgLatency = 0
      if(runningCount>0){
        if(runningCount>latWindowSize) // overflow, so we have removed N-latWindowSize'th request and added (N+1)th request, i.e. we only have latWindowSize number of requests..
          movingAvgLatency = cumulSum/latWindowSize
        else // We haven't yet populated the window, so we'll use first runningCount requests and use it as denominator. Can also use latWindowIdx though..
          movingAvgLatency = cumulSum/runningCount
      }
      //movingAvgLatency = if(runningCount>0) (cumulSum/runningCount) else 0
      if(runningCount>curAct_movWindow_numReqs) //movWindow_numReqs) 
        opZoneUpdate()      
    }
    numConts = toUpdateNumConts
    numWindowConts = toUpdateNumWindowConts
    logging.info(this,s"\t <adbg> <ASPI:update> action: ${actionName} invoker: ${myInvokerID}, latency: ${latency} count: ${runningCount} cumulSum: ${cumulSum} movingAvgLatency: ${movingAvgLatency} numConts: ${numConts} opZone: ${opZone} ")     
  }

}

class InvokerRunningState(var invkrInstId: InvokerInstanceId,var numInFlightReqs: Int, var curAvgLatency: Long,var curActOpZone:Int, var invokerRank:Int) extends functionInfo {
  var myScheduleState: Int = schedDefault
  var numConts: Int = 0
  var numWindowConts: Int = 0
  var weighedNumConts: Int = 0
  var updatedTS:Long = Instant.now.toEpochMilli

  def curScheduleState(standaloneRuntime: Long): Int = {
    //var latencyRatio: Double = (curInvokerStats.movingAvgLatency.toDouble/standaloneRuntime)
    //var toleranceRatio: Double = if(latencyRatio > 1.0) ((latencyRatio-1)/(latencyTolerance-1)) else safeBeginThreshold

    if( curActOpZone >= opZoneWarn){
      myScheduleState = schedRR
    }else{
      myScheduleState = schedDefault
    }
    myScheduleState
  }  

}

// the stats of an action across all invokers. Tracked per Invoker.
class ActionStats(val actionName:String,logging: Logging)extends functionInfo {
  var usedInvokers = mutable.Map.empty[InvokerInstanceId, AdapativeInvokerStats]
  //var lastInstantUsed = mutable.Map.empty[InvokerInstanceId, Long]
  var cmplxLastInstUsed = mutable.Map.empty[InvokerInstanceId, InvokerRunningState]
  //var inUseInvokers = mutable.Map.empty[InvokerInstanceId, InvokerRunningState]
  var inUseInvokers = mutable.Map.empty[Int, InvokerRunningState] // key: rank
  var inPlayInvokers = mutable.Map.empty[Int, InvokerRunningState] // key: rank, value: rank! only used to track invokers in cris!
  var prevUsedInvoker = 0
  var numTimesPrevUsedInvoker = 0

  val standaloneRuntime: Long = getFunctionRuntime(actionName)
  var numInvokers = 0; var nextInvokerToUse = 0;
  var shouldRemoveInvkr = mutable.Map.empty[Int, Int]
  //var dummyReqPeriod = 5 // period measured in number of requests //var sinceDummyIssued = 0; 
  var lastSqrProactiveTS: Long = 0

  def getAdjustedNumConts(curOpZone:Int,curNumConts:Int): Int = {
    var resNumConts = 0
    if( (curOpZone==opZoneUnSafe)){//} || (curOpZone==opZoneUndetermined)){
      resNumConts = 0
    }else if( (curOpZone==opZoneWarn)){ // || (curOpZone==opZoneUndetermined) ){
      resNumConts = 1
    }else{
      resNumConts = curNumConts
    }
    //logging.info(this,s"\t <adbg> <AS:gANC> action: ${actionName}, invokerNum: ${invokerNum} curOpZone: ${curOpZone} numConts: ${curNumConts} resNumConts: ${resNumConts}")
    resNumConts
    //curNumConts
  }

  def printAllContCount(toCheckInvoker:InvokerInstanceId): Unit={
    var curTime: Long = Instant.now.toEpochMilli
    var timeSinceLastSqrProactive = curTime - lastSqrProactiveTS  
    var minValidCountTime = curTime - statsTimeoutInMilli

    var totWindowConts = 0
    var totNumConts = 0
    var totWeighedConts = 0
    inUseInvokers.keys.foreach{
      curInvokerId =>
      inUseInvokers.get(curInvokerId.toInt) match {
        case Some(curInvokerRunningState) => 
          if( curInvokerRunningState.updatedTS > minValidCountTime){
            totWindowConts+= curInvokerRunningState.numWindowConts
            totNumConts+= curInvokerRunningState.numConts
            totWeighedConts+= curInvokerRunningState.weighedNumConts
            //totWeighedConts+= curInvokerRunningState.weighedNumConts
            logging.info(this,s"\t <adbg> <ISRPN-C1> action: ${actionName}, curInvokerId: ${curInvokerId.toInt} lastUpdated: ${curInvokerRunningState.updatedTS} numConts: ${curInvokerRunningState.numConts} windowConts: ${curInvokerRunningState.numWindowConts} weighedConts: ${curInvokerRunningState.weighedNumConts}")                  
          }else{
            logging.info(this,s"\t <adbg> <ISRPN-C2> action: ${actionName}, curInvokerId: ${curInvokerId.toInt} lastUpdated: ${curInvokerRunningState.updatedTS} numConts: ${curInvokerRunningState.numConts} windowConts: ${curInvokerRunningState.numWindowConts} weighedConts: ${curInvokerRunningState.weighedNumConts}")                              
          }
        case None =>
      }
    }
    var sqrtNumConts: Int = math.sqrt(totWindowConts).ceil.toInt
    if(sqrtNumConts==0) 
      sqrtNumConts = 1
    var reqdNumConts = totWindowConts + sqrtNumConts
    var shouldHaveReqstdNumExtraConts = reqdNumConts - totWeighedConts //reqdNumConts - totNumConts
    var numExtraContsNeeded = reqdNumConts - totWeighedConts //reqdNumConts - totNumConts
    // numReqsNumConts
    // TO-DO: Have not yet implemented forcedDummyReq! Hopefully with squareRootProactive we won't need it! 
    
    var numInvkrsConsidered = 0 
    // class isrpnMetadata(val proactivInvkr1:InvokerInstanceId,val proactivInvkr1NumReqs:Int,val proactivInvkr2:InvokerInstanceId,val proactivInvkr2NumReqs:Int){
    var proactivInvkr1: InvokerInstanceId = toCheckInvoker; var proactivInvkr2: InvokerInstanceId = toCheckInvoker;
    var proactivInvkr1NumReqs =0; var proactivInvkr2NumReqs = 0;
    var totDummyReqs = 0
    val shouldBeLastInvoker = if(inUseInvokers.size>0) (inUseInvokers.size-1) else 0 // 0-indexed, so inUseInvokers.size is the next invoker that'd be considered! // used for v0.51 to v0.55

    logging.info(this,s"\t <adbg> <ISRPN-1> action: ${actionName}, toCheckInvoker: ${toCheckInvoker.toInt} shouldBeLastInvoker: ${shouldBeLastInvoker} totNumConts: ${totNumConts} totWindowConts: ${totWindowConts} totWeighedConts: ${totWeighedConts} sqrtNumConts: ${sqrtNumConts} reqdNumConts: ${reqdNumConts} should-have-askedExtraConts: ${shouldHaveReqstdNumExtraConts} numExtraContsNeeded: ${numExtraContsNeeded}")
  }

  def addToInUse(invoker:InvokerInstanceId):Unit={
    // only called from isSquareRootProactiveNeeded
    // If called from condtn-1: i.e. fewer extra containers, shouldRemoveInvkr will be disabled on an invoker considered for extraSpawning
    // If called from condtn-2: this is a new invoker being added to inUse, so unlikley to be considered for shouldRemoveInvkr
    // If called from condtn-3: extra cont spawning is done on curInvoker, so it won't be considered for shouldRemoveInvkr

    inPlayInvokers.get(invoker.toInt) match{
      case Some(curInvokerRunningState) => 
        //logging.info(this,s"\t <adbg> <AS:atiu-1> action: ${actionName}, invoker: ${invoker.toInt} is PRESENT in inUseInvokers!")
      case None =>
        // Not checking for shouldRemoveInvkr because, 
        shouldRemoveInvkr.get(invoker.toInt) match{
          case Some(curNumInFlightReqs) =>
            shouldRemoveInvkr = shouldRemoveInvkr - invoker.toInt
            logging.info(this,s"\t <adbg> <AS:atiu-2> action: ${actionName}, invoker: ${invoker.toInt} is absent in inPlayInvokers since it is in shouldRemoveInvkr")
          case None =>
            logging.info(this,s"\t <adbg> <AS:atiu-2> action: ${actionName}, invoker: ${invoker.toInt} is absent in inPlayInvokers and in shouldRemoveInvkr so adding it to inPlayInvokers")
            inPlayInvokers = inPlayInvokers + (invoker.toInt -> new InvokerRunningState(invoker,0,0,0,invoker.toInt))
            var tempInPlayInvokerRunningState = inPlayInvokers(invoker.toInt)
            tempInPlayInvokerRunningState.numConts = 0; tempInPlayInvokerRunningState.numWindowConts = 0
        }         
    }
  }

  def addActionStats(invoker: InvokerInstanceId,invokerStats:AdapativeInvokerStats,latencyVal: Long,initTime: Long, toUpdateNumConts: Int,toUpdateWindowNumConts: Int){
    var curTime:Long = Instant.now.toEpochMilli
    usedInvokers.get(invoker) match{
      case Some(curInvokerStats) =>
        
        // updateActionStats(toUpdateAction:String, latencyVal: Long, toUpdateNumConts:Int):Unit = {
        var curOpZone = curInvokerStats.updateActionStats(actionName,latencyVal,initTime,toUpdateNumConts,toUpdateWindowNumConts)
        var adjustedNumConts = getAdjustedNumConts(curOpZone,toUpdateNumConts)
        var adjustedWindowNumConts = getAdjustedNumConts(curOpZone,toUpdateWindowNumConts)

        logging.info(this,s"\t <adbg> <AS:AAS> action: ${actionName}, invoker: ${invoker.toInt} is PRESENT. curTime: ${curTime} curOpZone: ${curOpZone} NumConts: ${adjustedNumConts} (${toUpdateNumConts}) windowConts: ${adjustedWindowNumConts} (${toUpdateWindowNumConts}) and curLatency: ${latencyVal} initTime: ${initTime}")
        inUseInvokers.get(invoker.toInt) match{
          case Some(curInvokerRunningState) => 
            curInvokerRunningState.numConts = toUpdateNumConts; 
            curInvokerRunningState.numWindowConts = toUpdateWindowNumConts
            curInvokerRunningState.weighedNumConts = adjustedNumConts
            curInvokerRunningState.updatedTS = curTime
            //printAllContCount(invoker)

            logging.info(this,s"\t <adbg> <AS:AAS-1.5> action: ${actionName}, invoker: ${invoker.toInt} is PRESENT in inUseInvokers!")
          case None =>
            logging.info(this,s"\t <adbg> <AS:AAS-1.6> action: ${actionName}, invoker: ${invoker.toInt} is absent in inUseInvokers, maybe adding it now...")

            shouldRemoveInvkr.get(invoker.toInt) match{
              case Some(curNumInFlightReqs) =>
                var tempVal = curNumInFlightReqs-1
                logging.info(this,s"\t <adbg> <AS:AAS-1.7> action: ${actionName}, invoker: ${invoker.toInt} is absent in inUseInvokers but present in shouldRemoveInvkr with ${curNumInFlightReqs} in-flight-reqs ")

                shouldRemoveInvkr = shouldRemoveInvkr + (invoker.toInt -> tempVal)
                shouldRemoveInvkr.get(invoker.toInt) match{
                  case Some(updatedNumInFlightReqs) =>
                    if(updatedNumInFlightReqs <= 0){
                      logging.info(this,s"\t <adbg> <AS:AAS-1.75> action: ${actionName}, invoker: ${invoker.toInt}, likely the last in-flight-req was processed, removing it from shouldRemoveInvkr ")                  
                      shouldRemoveInvkr = shouldRemoveInvkr - invoker.toInt
                    }
                  case None => 
                    logging.info(this,s"\t <adbg> <AS:AAS-1.76> action: ${actionName}, invoker: ${invoker.toInt}, likely the last in-flight-req was processed but it should not be coming here, FIX-IT!")                  
                }
              case None =>
                logging.info(this,s"\t <adbg> <AS:AAS-1.8> action: ${actionName}, invoker: ${invoker.toInt} is absent in inUseInvokers and shouldRemoveInvkr, so adding it to inUseInvokers")
                inUseInvokers = inUseInvokers + (invoker.toInt -> new InvokerRunningState(invoker,0,latencyVal,0,invoker.toInt))
                var tempInvokerRunningState = inUseInvokers(invoker.toInt)
                tempInvokerRunningState.numConts = toUpdateNumConts; 
                tempInvokerRunningState.numWindowConts = toUpdateWindowNumConts
                tempInvokerRunningState.weighedNumConts = adjustedNumConts
                tempInvokerRunningState.updatedTS = curTime
                inPlayInvokers = inPlayInvokers + (invoker.toInt -> inUseInvokers(invoker.toInt)) // hoping that I am referring to the same object!
                //printAllContCount(invoker)
            }                        
        }

      case None =>
        usedInvokers = usedInvokers + (invoker -> invokerStats)

        var curInstant: Long = Instant.now.toEpochMilli
        cmplxLastInstUsed = cmplxLastInstUsed + (invoker -> new InvokerRunningState(invoker,0,latencyVal,0,invoker.toInt))
        var tempInvokerStats:AdapativeInvokerStats  = usedInvokers(invoker)
        numInvokers+=1
        
        var curOpZone = tempInvokerStats.updateActionStats(actionName,latencyVal,initTime,toUpdateNumConts,toUpdateWindowNumConts)
        var adjustedNumConts = getAdjustedNumConts(curOpZone,toUpdateNumConts)
        var adjustedWindowNumConts = getAdjustedNumConts(curOpZone,toUpdateWindowNumConts)
        logging.info(this,s"\t <adbg> <AS:AAS> action: ${actionName}, invoker: ${invoker.toInt} is ABSENT, adding it to usedInvokers. NumConts: ${adjustedNumConts} (${toUpdateNumConts}) windowConts: ${adjustedWindowNumConts} (${toUpdateWindowNumConts}) and avgLat: ${latencyVal} at instant: ${curInstant} initTime: ${initTime}")
        // This section of code is an overkill, but safe nonetheless. Since it should not be possible that we not have curInvokerStats but would somehow be added to inUseInvokers, shouldRemoveInvkr etc..
        inUseInvokers.get(invoker.toInt) match{
          case Some(curInvokerRunningState) => 
            curInvokerRunningState.numConts = toUpdateNumConts; 
            curInvokerRunningState.numWindowConts = toUpdateWindowNumConts
            curInvokerRunningState.weighedNumConts = adjustedNumConts
            curInvokerRunningState.updatedTS = curTime
            //printAllContCount(invoker)

            logging.info(this,s"\t <adbg> <AS:AAS-1.1> action: ${actionName}, invoker: ${invoker.toInt} is PRESENT in inUseInvokers!")
          case None =>
            logging.info(this,s"\t <adbg> <AS:AAS-1.2> action: ${actionName}, invoker: ${invoker.toInt} is absent in inUseInvokers, maybe adding it now...")

            shouldRemoveInvkr.get(invoker.toInt) match{
              case Some(curNumInFlightReqs) =>
                var tempVal = curNumInFlightReqs-1
                logging.info(this,s"\t <adbg> <AS:AAS-1.3> action: ${actionName}, invoker: ${invoker.toInt} is absent in inUseInvokers but present in shouldRemoveInvkr with ${curNumInFlightReqs} in-flight-reqs ")

                shouldRemoveInvkr = shouldRemoveInvkr + (invoker.toInt -> tempVal)
                shouldRemoveInvkr.get(invoker.toInt) match{
                  case Some(updatedNumInFlightReqs) =>
                    if(updatedNumInFlightReqs <= 0){
                      logging.info(this,s"\t <adbg> <AS:AAS-1.35> action: ${actionName}, invoker: ${invoker.toInt}, likely the last in-flight-req was processed, removing it from shouldRemoveInvkr ")                  
                      shouldRemoveInvkr = shouldRemoveInvkr - invoker.toInt
                    }
                  case None => 
                    logging.info(this,s"\t <adbg> <AS:AAS-1.36> action: ${actionName}, invoker: ${invoker.toInt}, likely the last in-flight-req was processed but it should not be coming here, FIX-IT!")                  
                }
              case None =>
                logging.info(this,s"\t <adbg> <AS:AAS-1.4> action: ${actionName}, invoker: ${invoker.toInt} is absent in inUseInvokers and shouldRemoveInvkr, so adding it to inUseInvokers")
                inUseInvokers = inUseInvokers + (invoker.toInt -> new InvokerRunningState(invoker,0,latencyVal,0,invoker.toInt))
                var tempInvokerRunningState = inUseInvokers(invoker.toInt)
                tempInvokerRunningState.numConts = toUpdateNumConts; 
                tempInvokerRunningState.numWindowConts = toUpdateWindowNumConts
                tempInvokerRunningState.weighedNumConts = adjustedNumConts
                tempInvokerRunningState.updatedTS = curTime
                inPlayInvokers = inPlayInvokers + (invoker.toInt -> inUseInvokers(invoker.toInt)) // hoping that I am referring to the same object!
                //printAllContCount(invoker)
            }                        
        }      
    }
  }

  def initAllInvokers(invoker: InvokerInstanceId,invokerStats:AdapativeInvokerStats,latencyVal: Long,initTime: Long, toUpdateNumConts: Int){
    // not used to actually update stats.
    usedInvokers.get(invoker) match{
      case Some(curInvokerStats) =>
        logging.info(this,s"\t <adbg> <AS:AIUI-1> action: ${actionName}, invoker: ${invoker.toInt} is PRESENT. NumConts: ${toUpdateNumConts} and avgLat: ${latencyVal} initTime: ${initTime}")
        // updateActionStats(toUpdateAction:String, latencyVal: Long, toUpdateNumConts:Int):Unit = {
        //curInvokerStats.updateActionStats(actionName,latencyVal,initTime,toUpdateNumConts)

      case None =>
        // this is not needed, just here for completion.
        usedInvokers = usedInvokers + (invoker -> invokerStats)
        cmplxLastInstUsed = cmplxLastInstUsed + (invoker -> new InvokerRunningState(invoker,0,latencyVal,0,invoker.toInt))
        var tempInvokerStats:AdapativeInvokerStats  = usedInvokers(invoker)
        numInvokers+=1

        logging.info(this,s"\t <adbg> <AS:AIUI-2> action: ${actionName}, invoker: ${invoker.toInt} is ABSENT, adding it to usedInvokers. NumConts: ${toUpdateNumConts} and avgLat: ${latencyVal} initTime: ${initTime}")
        //tempInvokerStats.updateActionStats(actionName,latencyVal,initTime,toUpdateNumConts)
    }
  }

  def checkExpiredInvkrs(toRemoveBuf:ListBuffer[InvokerInstanceId]): Int = {
    var willRemoveInvkr: ListBuffer[Int] = new mutable.ListBuffer[Int] // keys..

    toRemoveBuf.foreach{
      curInvoker =>
      var curInvokerId = curInvoker.toInt
      inUseInvokers.get(curInvokerId) match{
        case Some(curInvokerRunningState) => 
          logging.info(this,s"\t <adbg> <AS:CEI:1> action: ${actionName}, invoker: ${curInvoker.toInt} is PRESENT in inUseInvokers!")
          
          usedInvokers.get(curInvoker) match {
            case Some(curInvokerStats) => 
              val foundNumConts = curInvokerStats.getCurActNumConts(actionName)    
              if(foundNumConts==0){
                willRemoveInvkr+=curInvokerId
              }              
              logging.info(this,s"\t <adbg> <CEI:1.2> Invoker: ${curInvoker.toInt} foundNumConts: ${foundNumConts}")
            case None => 
              logging.info(this,s"\t <adbg> <CEI:1.9> Invoker: ${curInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")
          }
        case None =>
          logging.info(this,s"\t <adbg> <AS:CEI:2> action: ${actionName}, invoker: ${curInvoker.toInt} is absent in inUseInvokers, not doing anything..")
      }    
    }

    willRemoveInvkr.foreach{
      curInvokerId => 
      logging.info(this,s"\t <adbg> <AS:CEI:3> action: ${actionName}'s container is evidentaly not running on invoker: ${curInvokerId}. Ideally should remove it..")
    }
    
    numInvokers = inUseInvokers.size 
    if(numInvokers==0) numInvokers=1
    numInvokers 
  }

  def checkRunningInvkrStatus(): Int={
    // Can assume that we have 
    //var rankOrderedInvokers = ListMap(inUseInvokers.toSeq.sortWith(_._2.invokerRank < _._2.invokerRank):_*) 
    var rankOrderedInvokers = ListMap(inPlayInvokers.toSeq.sortWith(_._2.invokerRank < _._2.invokerRank):_*) 
    var numNonWarnInvkrs:Int = 0
    var lastInvkrInFlightReqs: Int = 0; var lastInvkrId = 0
    rankOrderedInvokers.keys.foreach{
      curInvokerId => 
      //inUseInvokers.get(curInvokerId) match{
      inPlayInvokers.get(curInvokerId) match{
        case Some(curInvokerRunningState) => 
          var curInvoker = curInvokerRunningState.invkrInstId    
          logging.info(this,s"\t <adbg> <AS:CRI:1> action: ${actionName}, invoker: ${curInvoker.toInt} is PRESENT in inUseInvokers!")
          usedInvokers.get(curInvoker) match {
            case Some(curInvokerStats) => 
              //val (numConts,thisActOpZone,lastUpdated,actLatency) = curInvokerStats.findActionNumContsOpZone(actionName)    
              val pAMII = curInvokerStats.getActMetadataInInvkr(actionName) // pAMII --> presentActMetadataInInvkr
              lastInvkrInFlightReqs = pAMII.numInFlightReqs
              lastInvkrId = curInvokerId

              if(pAMII.thisActOpZone < opZoneWarn)
                numNonWarnInvkrs+=1
              logging.info(this,s"\t <adbg> <cris:1.2> Invoker: ${curInvoker.toInt} numConts: ${pAMII.numConts} thisActOpZone: ${pAMII.thisActOpZone} actLatency: ${pAMII.actLatency} lastInvkrInFlightReqs: ${lastInvkrInFlightReqs} numNonWarnInvkrs: ${numNonWarnInvkrs} ")
            case None => 
              logging.info(this,s"\t <adbg> <cris:1.9> Invoker: ${curInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")
          }
        case None =>
          logging.info(this,s"\t <adbg> <AS:cris:2> action: ${actionName}, invoker: ${curInvokerId} is absent in inUseInvokers, not doing anything..")
      } 
    }
    var numInvokers = inPlayInvokers.size //inUseInvokers.size
    val curNonWarnRatio:Double = if(numInvokers==0) 0 else numNonWarnInvkrs.toDouble/numInvokers

    logging.info(this,s"\t <adbg> <cris:3> curNonWarnRatio: ${curNonWarnRatio} minNonWarnRatio: ${minNonWarnRatio} numInvokers: ${numInvokers}")      
    if( (curNonWarnRatio >= minNonWarnRatio) && (numInvokers>2)){
      inUseInvokers.get(lastInvkrId) match{
        case Some(lastInvkrRunningState) =>
          /*shouldRemoveInvkr = shouldRemoveInvkr + (lastInvkrId -> lastInvkrInFlightReqs)  
          inUseInvokers = inUseInvokers - lastInvkrId
          nextInvokerToUse = 0 // will reset the RR-loop this way.. */
          logging.info(this,s"\t <adbg> <cris:3.1> action: ${actionName} curNonWarnRatio: ${curNonWarnRatio} is greater than minNonWarnRatio: ${minNonWarnRatio}. So invoker: ${lastInvkrId} with ${lastInvkrInFlightReqs} in-flight-reqs is scheduled to be removed ${shouldRemoveInvkr(lastInvkrId)} ")      
        case None =>
          logging.info(this,s"\t <adbg> <cris:3.2> action: ${actionName} curNonWarnRatio: ${curNonWarnRatio} is greater than minNonWarnRatio: ${minNonWarnRatio}. But invoker: ${lastInvkrId} is not found in inUseInvokers, so somethings is fishy.. ")      
      }
    }
    inUseInvokers.size
  }

  def getAutoScaleUsedInvoker(numProactiveContsToSpawn: Int,curInvokerProactiveContsToSpawn: Int): Option[(InvokerInstanceId,allocMetadata)] = {
    //var rankOrderedInvokers = ListMap(cmplxLastInstUsed.toSeq.sortWith(_._2.invokerRank < _._2.invokerRank):_*) // OG-AUTOSCALE!
    var rankOrderedInvokers = ListMap(inUseInvokers.toSeq.sortWith(_._2.invokerRank < _._2.invokerRank):_*) // OG-AUTOSCALE!

    var lastCheckedInvoker: Int = -1
    var numInvokers: Int = inUseInvokers.size
    var curIterInvokerRank: Int = nextInvokerToUse
    for(curIterIdx <- 1 to numInvokers){
      // should check whether rank is within #members?
      var curInvokerRunningState = inUseInvokers(curIterInvokerRank)
      var curInvokerSchedState = curInvokerRunningState.curScheduleState(standaloneRuntime)

      logging.info(this,s"\t <adbg> <gasUI:a> action: ${actionName}, curIterInvokerRank: ${curIterInvokerRank} curIterIdx: ${curIterIdx} numInvokers: ${numInvokers} curInvokerSchedState: ${curInvokerSchedState}")
      val curInvoker = curInvokerRunningState.invkrInstId 
      usedInvokers.get(curInvoker) match {
        case Some(curInvokerStats) => 
          logging.info(this,s"\t <adbg> <gasUI:b> action: ${actionName}, invoker: ${curInvokerRunningState.invokerRank} has #inflight-reqs: ${curInvokerRunningState.numInFlightReqs} numInvokers: ${numInvokers} curIterIdx: ${curIterIdx} numInvokers: ${numInvokers} checking whether it has any capacityRemaining...")
          // If I fit, I will choose this.
          // TODO: Change this so that I iterate based on some "ranking"
          //var curInvokerStats: AdapativeInvokerStats = usedInvokers(curInvoker)
          val (numInFlightReqs,curAvgLatency,curActOpZone,decision) = curInvokerStats.capacityRemaining(actionName)
          curInvokerRunningState.numInFlightReqs = numInFlightReqs
          curInvokerRunningState.curAvgLatency = curAvgLatency
          curInvokerRunningState.curActOpZone = curActOpZone

          if(decision){
            if(prevUsedInvoker == curInvoker.toInt){
              numTimesPrevUsedInvoker+=1
            }
            else{
              numTimesPrevUsedInvoker= 1
              prevUsedInvoker = curInvoker.toInt
            }

            numInvokers = inUseInvokers.size 
            // WARNING: Assuming that invokers will go in increasing order..
            if(numInvokers==0) numInvokers=1
            if(curInvokerSchedState==1){
              var temp = (curInvokerRunningState.invokerRank+1).toInt
              logging.info(this,s"\t <adbg> <gasUI:findNextInvkr> curInvokerRunningState.invokerRank: ${curInvokerRunningState.invokerRank} lhs: ${temp} numInvokers: ${numInvokers}")                
              nextInvokerToUse = (temp)%(numInvokers)
            }

            if(numTimesPrevUsedInvoker>= prevUsedThreshold){ // Ensuring the earliest nodes are used instead of always using the last node. 
              nextInvokerToUse = 0 // Trying to force RR this way..
              numTimesPrevUsedInvoker = 0 
              logging.info(this,s"\t <adbg> <gasUI:resetnextInvokerToUse> action: ${actionName} invoker: ${curInvoker.toInt} numInvokers: ${numInvokers} nextInvokerToUse: ${nextInvokerToUse} prevUsedInvoker: ${prevUsedInvoker} numTimesPrevUsedInvoker: ${numTimesPrevUsedInvoker}")                  
            }

            nextInvokerToUse = 0 // removing the RR behavior, to see performance.

            logging.info(this,s"\t <adbg> <gasUI:c> action: ${actionName} invoker: ${curInvoker.toInt} numInvokers: ${numInvokers} nextInvokerToUse: ${nextInvokerToUse} prevUsedInvoker: ${prevUsedInvoker} numTimesPrevUsedInvoker: ${numTimesPrevUsedInvoker}")  
            val toUseProactiveInvokerId = (curInvoker.toInt+1)%cmplxLastInstUsed.size
            //val curReqIaspnMetadata = isSquareRootProactiveNeeded(curInvoker,toUseProactiveInvokerId,false,curInvokerProactiveContsToSpawn,numProactiveContsToSpawn); var curAllocMetadata = new allocMetadata(curInvoker,curReqIaspnMetadata)
            val curReqIsrpnMetadata = isSquareRootProactiveNeeded(curInvoker,toUseProactiveInvokerId,false,curInvokerProactiveContsToSpawn,numProactiveContsToSpawn)
            var curAllocMetadata = new allocMetadata(curInvoker,curReqIsrpnMetadata)
            return Some(curInvoker,curAllocMetadata)
          } 
        case None =>
          logging.info(this,s"\t <adbg> <getUsedInvoker> Invoker: ${curInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")
      }
      curIterInvokerRank = (curIterInvokerRank+1)%numInvokers
    }
    logging.info(this,s"\t <adbg> <gasUI> action: ${actionName} did not get a used invoker :( :( ")
    getActiveInvoker(lastCheckedInvoker,numProactiveContsToSpawn,curInvokerProactiveContsToSpawn)
    //None
  }

  def isSquareRootProactiveNeeded(toCheckInvoker: InvokerInstanceId,nextInvokerId: Int,forcedDummyReq: Boolean,myProactiveMaxReqs: Int,nextInvokerMaxProactiveReqs: Int): isrpnMetadata ={
    // forcedDummyReq ==> generated because we have to spawn a new invoker and all the prev invokers are running at fully capacity!

    val tempRankOrderedInvokers = ListMap(cmplxLastInstUsed.toSeq.sortWith(_._2.invokerRank < _._2.invokerRank):_*) // OG-AUTOSCALE!    
    val rankSortedInvokers = tempRankOrderedInvokers.keys.toList

    var curTime: Long = Instant.now.toEpochMilli
    var timeSinceLastSqrProactive = curTime - lastSqrProactiveTS  
    var minValidCountTime = curTime - statsTimeoutInMilli
    var totWindowConts = 0; //inUseInvokers.foreach(x => totWindowConts+= x._2.numWindowConts)
    var totNumConts = 0; //inUseInvokers.foreach(x => totNumConts+= x._2.numConts)
    var totWeighedConts = 0; //inUseInvokers.foreach(x => totWeighedConts+= x._2.weighedNumConts)

    //inUseInvokers.keys.foreach{
    rankSortedInvokers.foreach{
      curInvokerId =>
      inUseInvokers.get(curInvokerId.toInt) match {
        case Some(curInvokerRunningState) => 
          if( curInvokerRunningState.updatedTS > minValidCountTime){
            totWindowConts+= curInvokerRunningState.numWindowConts
            totNumConts+= curInvokerRunningState.numConts
            totWeighedConts+= curInvokerRunningState.weighedNumConts
            //totWeighedConts+= curInvokerRunningState.weighedNumConts
            logging.info(this,s"\t <adbg> <ISRPN-C1> action: ${actionName}, curInvokerId: ${curInvokerId.toInt} lastUpdated: ${curInvokerRunningState.updatedTS} numConts: ${curInvokerRunningState.numConts} windowConts: ${curInvokerRunningState.numWindowConts} weighedConts: ${curInvokerRunningState.weighedNumConts}")                  
          }else{
            //logging.info(this,s"\t <adbg> <ISRPN-C2> action: ${actionName}, curInvokerId: ${curInvokerId.toInt} lastUpdated: ${curInvokerRunningState.updatedTS} numConts: ${curInvokerRunningState.numConts} windowConts: ${curInvokerRunningState.numWindowConts} weighedConts: ${curInvokerRunningState.weighedNumConts}")                              
          }
        case None =>
      }
    }

    var sqrtNumConts: Int = math.sqrt(totWindowConts).ceil.toInt
    if(sqrtNumConts==0) 
      sqrtNumConts = 1
    var reqdNumConts = totWindowConts + sqrtNumConts
    // var shouldHaveReqstdNumExtraConts = reqdNumConts - totWeighedConts //reqdNumConts - totNumConts
    // var numExtraContsNeeded = reqdNumConts - totWeighedConts //reqdNumConts - totNumConts
    var shouldHaveReqstdNumExtraConts = reqdNumConts - totNumConts
    var numExtraContsNeeded = reqdNumConts - totNumConts
    // numReqsNumConts
    // TO-DO: Have not yet implemented forcedDummyReq! Hopefully with squareRootProactive we won't need it! 
    
    var numInvkrsConsidered = 0 
    // class isrpnMetadata(val proactivInvkr1:InvokerInstanceId,val proactivInvkr1NumReqs:Int,val proactivInvkr2:InvokerInstanceId,val proactivInvkr2NumReqs:Int){
    var proactivInvkr1: InvokerInstanceId = toCheckInvoker; var proactivInvkr2: InvokerInstanceId = toCheckInvoker;
    var proactivInvkr1NumReqs =0; var proactivInvkr2NumReqs = 0;
    var totDummyReqs = 0
    val shouldBeLastInvoker = if(inUseInvokers.size>0) (inUseInvokers.size-1) else 0 // 0-indexed, so inUseInvokers.size is the next invoker that'd be considered! // used for v0.51 to v0.55

    logging.info(this,s"\t <adbg> <ISRPN-1> action: ${actionName}, toCheckInvoker: ${toCheckInvoker.toInt} shouldBeLastInvoker: ${shouldBeLastInvoker} totNumConts: ${totNumConts} totWindowConts: ${totWindowConts} totWeighedConts: ${totWeighedConts} sqrtNumConts: ${sqrtNumConts} reqdNumConts: ${reqdNumConts} should-have-askedExtraConts: ${shouldHaveReqstdNumExtraConts} numExtraContsNeeded: ${numExtraContsNeeded}")
    if(forcedDummyReq){
        var curInvkrNumCores = 0; var curInvkrReqs = 0; var curInvkrNumConts = 0; 
        var curActTypeNumConts = 0; var curActTypeMaxConts = 0;
        var curNumDummyReqs = 0; var curInvkrOpZone = opZoneUnSafe
        usedInvokers.get(toCheckInvoker) match {
          case Some(toCheckInvokerStats) =>
            val relevantStats = toCheckInvokerStats.numCoresReqsContsDummyReqsOpZone(actionName)
            curInvkrNumCores = relevantStats._1 
            curInvkrReqs = relevantStats._2
            curInvkrNumConts = relevantStats._3
            curActTypeNumConts = relevantStats._4
            curActTypeMaxConts = relevantStats._5
            curNumDummyReqs = relevantStats._6
            curInvkrOpZone = relevantStats._7

            logging.info(this,s"\t <adbg> <ISRPN-8.0> action: ${actionName}, currently numExtraContsNeeded: ${numExtraContsNeeded} toCheckInvoker: ${toCheckInvoker.toInt} cores: ${curInvkrNumCores} reqs: ${curInvkrReqs} conts: ${curInvkrNumConts} curInvkrOpZone: ${curInvkrOpZone} forcedDummyReq: ${forcedDummyReq}")
            // implies existing invoker cannot accommodate any new request, will add a new one, just in case.
          case None =>
            logging.info(this,s"\t <adbg> <ISRPN-8.1> action: ${actionName}, this should NOT be happening, FIX-IT!!")
        } 

        var c1:Boolean = forcedDummyReq
        var c2:Boolean = (curInvkrOpZone >= opZoneSpawnInNewInvoker)

        var nextInvokerRank = (shouldBeLastInvoker.toInt+1)%(usedInvokers.size)
        logging.info(this,s"\t <adbg> <ISRPN-F2> action: ${actionName}, curInvkrReqs: ${curInvkrReqs} is equal or greater than curInvkrNumCores: ${curInvkrNumCores}. Since this invoker is exhausted, we will add an extraInvoker: ${nextInvokerRank}!")
        rankSortedInvokers.foreach{
          curSortedInvoker =>
          if(curSortedInvoker.toInt == nextInvokerRank){

            usedInvokers.get(curSortedInvoker) match {
              case Some(curSortedInvokerStats) =>

                val (curInvkrNumCores, curInvkrReqs,curInvkrNumConts,curActTypeNumConts,curActTypeMaxConts,curNumDummyReqs,curInvkrOpZone) = curSortedInvokerStats.numCoresReqsContsDummyReqsOpZone(actionName)
                val accumDummyReqsForCurAct = curSortedInvokerStats.getCurActAccumDummyReqs(actionName)
                var maxContCapacity = curActTypeMaxConts - curActTypeNumConts
                logging.info(this,s"\t <adbg> <ISRPN-FPRE> conts: ${curInvkrNumConts} actType-conts: ${curActTypeNumConts} curActTypeMaxConts: ${curActTypeMaxConts} maxContCapacity: ${maxContCapacity} accumDummyReqsForCurAct: ${accumDummyReqsForCurAct}")
                //if( (curNumDummyReqs<=(curInvkrNumCores/2).toInt) && (curNumDummyReqs<=maxContCapacity) ) {
                //if( (curNumDummyReqs<=(curInvkrNumCores/2).toInt) ) {
                if(accumDummyReqsForCurAct==0){
                  proactivInvkr2 = curSortedInvoker
                  proactivInvkr2NumReqs = 1
                  totDummyReqs+=proactivInvkr2NumReqs
                  logging.info(this,s"\t <adbg> <ISRPN-F3> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} is the same as nextInvokerRank: ${nextInvokerRank}, so adding ${proactivInvkr2NumReqs} dummy reqs")

                  shouldRemoveInvkr.get(curSortedInvoker.toInt) match{
                    case Some(curNumInFlightReqs) =>
                      shouldRemoveInvkr = shouldRemoveInvkr - curSortedInvoker.toInt
                      logging.info(this,s"\t <adbg> <AS:ISRPN-F3.0.1> action: ${actionName}, invoker: ${curSortedInvoker.toInt} is scheduled to be removed, but it is being overriden now!")
                    case None =>
                      logging.info(this,s"\t <adbg> <AS:ISRPN-F3.0.2> action: ${actionName}, invoker: ${curSortedInvoker.toInt} is not scheuled to be removed, so go ahead consider it for proactive spawning!")
                  }                   
                  curSortedInvokerStats.addDummyReq(actionName,proactivInvkr2NumReqs)
                  logging.info(this,s"\t <adbg> <ISRPN-F3.1> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} has ${proactivInvkr2NumReqs} more dummy reqs now, along with previous ${curNumDummyReqs} requests.")
                }else{
                  logging.info(this,s"\t <adbg> <ISRPN-F3.2> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} has ${curNumDummyReqs} tracked dummy reqs")
                }

              case None =>
                logging.info(this,s"\t <adbg> <ISRPN-F3.3> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} tried to add ${proactivInvkr2NumReqs} tracked dummy reqs but couldn't find invoker's AdapativeInvokerStats object! FIX-IT!!!")
            }
          }else{
            //logging.info(this,s"\t <adbg> <ISRPN-F4> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} is not-the same as nextInvokerRank: ${nextInvokerRank}")
          }
        }
    }
 
    
    if( (numExtraContsNeeded>0) && (timeSinceLastSqrProactive > minProactivWaitTime ) ){
      rankSortedInvokers.foreach{
        nextInvoker =>

        if( (numInvkrsConsidered<2) && (numExtraContsNeeded>0) && (nextInvoker.toInt >= toCheckInvoker.toInt) && (nextInvoker.toInt <= shouldBeLastInvoker) ) {

          usedInvokers.get(nextInvoker) match {

            case Some(nextInvokerStats) =>
              val (curInvkrNumCores, curInvkrReqs,curInvkrNumConts,curActTypeNumConts,curActTypeMaxConts,curNumDummyReqs,curInvkrOpZone) = nextInvokerStats.numCoresReqsContsDummyReqsOpZone(actionName)
              logging.info(this,s"\t <adbg> <ISRPN-2> action: ${actionName}, currently numExtraContsNeeded: ${numExtraContsNeeded} nextInvkr: ${nextInvoker.toInt} cores: ${curInvkrNumCores} reqs: ${curInvkrReqs} conts: ${curInvkrNumConts} curInvkrOpZone: ${curInvkrOpZone}")
              var scheduledToBeRemoved = 0;

              shouldRemoveInvkr.get(nextInvoker.toInt) match{
                case Some(curNumInFlightReqs) =>
                  shouldRemoveInvkr = shouldRemoveInvkr - nextInvoker.toInt
                  logging.info(this,s"\t <adbg> <AS:ISRPN-2.1> action: ${actionName}, invoker: ${nextInvoker.toInt} is scheduled to be removed, but it is being overriden now!")
                case None =>
                  scheduledToBeRemoved = 0
                  //logging.info(this,s"\t <adbg> <AS:ISRPN-2.8> action: ${actionName}, invoker: ${nextInvoker.toInt} is not scheuled to be removed, so go ahead consider it for proactive spawning!")
              } 

              var minDummyReqs = curInvkrNumConts - curInvkrReqs // need atleast these many dummy reqs to create a new container.
              var maxDummyReqs = curInvkrNumCores - curInvkrReqs // should not send more than these many dummy requests.
              var maxContCapacity = curActTypeMaxConts - curActTypeNumConts
              if(maxDummyReqs>maxContCapacity) maxDummyReqs = maxContCapacity

              var likelyDummyReqs = maxDummyReqs - minDummyReqs
              if(curInvkrNumConts==0) minDummyReqs = 0
              
              logging.info(this,s"\t <adbg> <ISRPN-3> action: ${actionName}, currently numExtraContsNeeded: ${numExtraContsNeeded} nextInvkr: ${nextInvoker.toInt} cores: ${curInvkrNumCores} reqs: ${curInvkrReqs} conts: ${curInvkrNumConts} actType-conts: ${curActTypeNumConts} curNumDummyReqs: ${curNumDummyReqs} scheduledToBeRemoved: ${scheduledToBeRemoved}")
              logging.info(this,s"\t <adbg> <ISRPN-3.1> conts: ${curInvkrNumConts} actType-conts: ${curActTypeNumConts} curActTypeMaxConts: ${curActTypeMaxConts} maxContCapacity: ${maxContCapacity} bef-max-dummyreqs: ${curInvkrNumCores - curInvkrReqs}")
              if( (minDummyReqs >= 0) && (maxDummyReqs>0)  && (likelyDummyReqs>0) && (curInvkrNumConts< curInvkrNumCores) && (scheduledToBeRemoved==0) && (curNumDummyReqs==0)){

                var numDummyReqs = 0
                if( (minDummyReqs + numExtraContsNeeded) < maxDummyReqs ){
                  numDummyReqs = minDummyReqs + numExtraContsNeeded
                }else{
                  numDummyReqs = maxDummyReqs
                }        

                if(numInvkrsConsidered==0){
                  proactivInvkr1 = nextInvoker
                  proactivInvkr1NumReqs = numDummyReqs
                }else{
                  proactivInvkr2 = nextInvoker
                  proactivInvkr2NumReqs = numDummyReqs
                }

                totDummyReqs+=numDummyReqs
                if(likelyDummyReqs>numDummyReqs)
                  likelyDummyReqs = numDummyReqs
                numExtraContsNeeded = numExtraContsNeeded - likelyDummyReqs
                numInvkrsConsidered+=1 
                logging.info(this,s"\t <adbg> <ISRPN-4> action: ${actionName},  numExtraContsNeeded: ${numExtraContsNeeded} nextInvkr: ${nextInvoker.toInt} numInvkrsConsidered: ${numInvkrsConsidered} numDummyReqs: ${numDummyReqs} minDummyReqs: ${minDummyReqs} maxDummyReqs: ${maxDummyReqs} likelyDummyReqs: ${likelyDummyReqs}")

                nextInvokerStats.addDummyReq(actionName,numDummyReqs)
              }else{
                if(curNumDummyReqs>0)
                  numExtraContsNeeded = numExtraContsNeeded - curNumDummyReqs
                logging.info(this,s"\t <adbg> <ISRPN-5> action: ${actionName},  numExtraContsNeeded: ${numExtraContsNeeded} nextInvkr: ${nextInvoker.toInt} minDummyReqs: ${minDummyReqs} maxDummyReqs: ${maxDummyReqs} likelyDummyReqs: ${likelyDummyReqs} curNumDummyReqs: ${curNumDummyReqs}")
              }
            case None =>
              logging.info(this,s"\t <adbg> <ISRPN> 6.0 Invoker: ${nextInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..") 
          }

        }
        else{
          //logging.info(this,s"\t <adbg> <ISRPN-7> action: ${actionName}, numInvkrsConsidered: ${numInvkrsConsidered} numExtraContsNeeded: ${numExtraContsNeeded} toCheckInvkr: ${toCheckInvoker.toInt} nxtInvkr: ${nextInvoker.toInt} shouldBeLastInvoker: ${shouldBeLastInvoker} ")      
        }

      } 

      if( (proactivInvkr1NumReqs!=0) || (proactivInvkr2NumReqs!=0)){ // implies a proactive request will be issued.      
        lastSqrProactiveTS = curTime
      }
    }
    if(totDummyReqs==0){

      var curInvkrNumCores = 0; var curInvkrReqs = 0; var curInvkrNumConts = 0; 
      var curActTypeNumConts = 0; var curActTypeMaxConts = 0;
      var curNumDummyReqs = 0; var curInvkrOpZone = opZoneUnSafe
      usedInvokers.get(toCheckInvoker) match {
        case Some(toCheckInvokerStats) =>
          val relevantStats = toCheckInvokerStats.numCoresReqsContsDummyReqsOpZone(actionName)
          curInvkrNumCores = relevantStats._1 
          curInvkrReqs = relevantStats._2
          curInvkrNumConts = relevantStats._3
          curActTypeNumConts = relevantStats._4
          curActTypeMaxConts = relevantStats._5
          curNumDummyReqs = relevantStats._6
          curInvkrOpZone = relevantStats._7

          logging.info(this,s"\t <adbg> <ISRPN-8.0> action: ${actionName}, currently numExtraContsNeeded: ${numExtraContsNeeded} toCheckInvoker: ${toCheckInvoker.toInt} cores: ${curInvkrNumCores} reqs: ${curInvkrReqs} conts: ${curInvkrNumConts} curInvkrOpZone: ${curInvkrOpZone} forcedDummyReq: ${forcedDummyReq}")
          // implies existing invoker cannot accommodate any new request, will add a new one, just in case.
        case None =>
          logging.info(this,s"\t <adbg> <ISRPN-8.1> action: ${actionName}, this should NOT be happening, FIX-IT!!")
      } 

      var c1:Boolean = forcedDummyReq
      var c2:Boolean = (curInvkrOpZone >= opZoneSpawnInNewInvoker)

      if(forcedDummyReq){

          var nextInvokerRank = (shouldBeLastInvoker.toInt+1)%(usedInvokers.size)
          logging.info(this,s"\t <adbg> <ISRPN-F2> action: ${actionName}, curInvkrReqs: ${curInvkrReqs} is equal or greater than curInvkrNumCores: ${curInvkrNumCores}. Since this invoker is exhausted, we will add an extraInvoker: ${nextInvokerRank}!")
          rankSortedInvokers.foreach{
            curSortedInvoker =>
            if(curSortedInvoker.toInt == nextInvokerRank){

              usedInvokers.get(curSortedInvoker) match {
                case Some(curSortedInvokerStats) =>

                  val (curInvkrNumCores, curInvkrReqs,curInvkrNumConts,curActTypeNumConts,curActTypeMaxConts,curNumDummyReqs,curInvkrOpZone) = curSortedInvokerStats.numCoresReqsContsDummyReqsOpZone(actionName)
                  val accumDummyReqsForCurAct = curSortedInvokerStats.getCurActAccumDummyReqs(actionName)
                  var maxContCapacity = curActTypeMaxConts - curActTypeNumConts
                  logging.info(this,s"\t <adbg> <ISRPN-FPRE> conts: ${curInvkrNumConts} actType-conts: ${curActTypeNumConts} curActTypeMaxConts: ${curActTypeMaxConts} maxContCapacity: ${maxContCapacity} accumDummyReqsForCurAct: ${accumDummyReqsForCurAct}")
                  //if( (curNumDummyReqs<=(curInvkrNumCores/2).toInt) && (curNumDummyReqs<=maxContCapacity) ) {
                  //if( (curNumDummyReqs<=(curInvkrNumCores/2).toInt) ) {
                  if(accumDummyReqsForCurAct==0){
                    proactivInvkr2 = curSortedInvoker
                    proactivInvkr2NumReqs = 1
                    totDummyReqs+=proactivInvkr2NumReqs
                    logging.info(this,s"\t <adbg> <ISRPN-F3> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} is the same as nextInvokerRank: ${nextInvokerRank}, so adding ${proactivInvkr2NumReqs} dummy reqs")

                    shouldRemoveInvkr.get(curSortedInvoker.toInt) match{
                      case Some(curNumInFlightReqs) =>
                        shouldRemoveInvkr = shouldRemoveInvkr - curSortedInvoker.toInt
                        logging.info(this,s"\t <adbg> <AS:ISRPN-F3.0.1> action: ${actionName}, invoker: ${curSortedInvoker.toInt} is scheduled to be removed, but it is being overriden now!")
                      case None =>
                        logging.info(this,s"\t <adbg> <AS:ISRPN-F3.0.2> action: ${actionName}, invoker: ${curSortedInvoker.toInt} is not scheuled to be removed, so go ahead consider it for proactive spawning!")
                    }                   
                    curSortedInvokerStats.addDummyReq(actionName,proactivInvkr2NumReqs)
                    logging.info(this,s"\t <adbg> <ISRPN-F3.1> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} has ${proactivInvkr2NumReqs} more dummy reqs now, along with previous ${curNumDummyReqs} requests.")
                  }else{
                    logging.info(this,s"\t <adbg> <ISRPN-F3.2> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} has ${curNumDummyReqs} tracked dummy reqs")
                  }

                case None =>
                  logging.info(this,s"\t <adbg> <ISRPN-F3.3> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} tried to add ${proactivInvkr2NumReqs} tracked dummy reqs but couldn't find invoker's AdapativeInvokerStats object! FIX-IT!!!")
              }
            }else{
              //logging.info(this,s"\t <adbg> <ISRPN-F4> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} is not-the same as nextInvokerRank: ${nextInvokerRank}")
            }
          }
      }
      /*else if(curInvkrOpZone >= opZoneSpawnInNewInvoker){
        //if(curInvkrReqs>=curInvkrNumCores){
          var nextInvokerRank = (toCheckInvoker.toInt+1)%(usedInvokers.size)
          logging.info(this,s"\t <adbg> <ISRPN-8.2> action: ${actionName}, curInvkrReqs: ${curInvkrReqs} is equal or greater than curInvkrNumCores: ${curInvkrNumCores}. Since this invoker is exhausted, we will add an extraInvoker: ${nextInvokerRank}!")

          rankSortedInvokers.foreach{
            curSortedInvoker =>
            if(curSortedInvoker.toInt == nextInvokerRank){
              usedInvokers.get(curSortedInvoker) match {
                case Some(curSortedInvokerStats) =>
                  var checkIfSpawnPossible = curSortedInvokerStats.canSquareRootDummyReqBeIssued(actionName,curTime)
                  if(checkIfSpawnPossible){
                    proactivInvkr2 = curSortedInvoker
                    proactivInvkr2NumReqs = zoneBasedProactivReqs
                    totDummyReqs+=proactivInvkr2NumReqs
                    logging.info(this,s"\t <adbg> <ISRPN-8.3> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} is the same as nextInvokerRank: ${nextInvokerRank}, so adding ${proactivInvkr2NumReqs} dummy reqs")
                    curSortedInvokerStats.addDummyReq(actionName,proactivInvkr2NumReqs)
                  }else{
                    logging.info(this,s"\t <adbg> <ISRPN-8.31> action: ${actionName}, evidently checkIfSpawnPossible: ${checkIfSpawnPossible} is returned false for curSortedInvoker: ${curSortedInvoker.toInt} else would have sent ${zoneBasedProactivReqs} dummy reqs") 
                  }
                case None =>
                  logging.info(this,s"\t <adbg> <ISRPN-8.32> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} tried to add ${proactivInvkr2NumReqs} tracked dummy reqs but couldn't find invoker's AdapativeInvokerStats object! FIX-IT!!!")
              }
            }else{
              logging.info(this,s"\t <adbg> <ISRPN-8.4> action: ${actionName}, curSortedInvoker: ${curSortedInvoker.toInt} is not-the same as nextInvokerRank: ${nextInvokerRank}")
            }
          }
        /*}else{
          logging.info(this,s"\t <adbg> <ISRPN-8.5> action: ${actionName}, curInvkrReqs: ${curInvkrReqs} curInvkrNumCores: ${curInvkrNumCores}")
        }*/ 
      }else{
        logging.info(this,s"\t <adbg> <ISRPN-8.5> action: ${actionName}, toCheckInvoker: ${toCheckInvoker.toInt} forcedDummyReq: ${forcedDummyReq} curInvkrOpZone: ${curInvkrOpZone}" )
      }*/
    }
 
    if( (proactivInvkr1NumReqs!=0) || (proactivInvkr2NumReqs!=0)){ // implies a proactive request will be issued.
      if(proactivInvkr1NumReqs>0){
        addToInUse(proactivInvkr1)
      }
      if(proactivInvkr2NumReqs>0){
        addToInUse(proactivInvkr2)
      }
    }
    logging.info(this,s"\t <adbg> <ISRPN-END> action: ${actionName}, proactivInvkr1: ${proactivInvkr1.toInt} proactivInvkr1NumReqs: ${proactivInvkr1NumReqs} proactivInvkr2: ${proactivInvkr2.toInt} proactivInvkr2NumReqs: ${proactivInvkr2NumReqs} lastSqrProactiveTS: ${lastSqrProactiveTS} totDummyReqs: ${totDummyReqs}")
    val retData = new isrpnMetadata(proactivInvkr1,proactivInvkr1NumReqs,proactivInvkr2,proactivInvkr2NumReqs)
    return retData 
  }

  //def getActiveInvoker(activeInvokers: ListBuffer[InvokerHealth]): Option[InvokerInstanceId] = {
  def getActiveInvoker(lastCheckedInvoker: Int,numProactiveContsToSpawn: Int,curInvokerProactiveContsToSpawn: Int): Option[(InvokerInstanceId,allocMetadata)] = {
    // ASSUMPTION: cmplxLastInstUsed has all the invokers? 
    // If it has come here means, all the prev invokers are loaded, so we will choose the least loaded invoker and spawn a proactive container in a new node.
    var rankOrderedInvokers = ListMap(inUseInvokers.toSeq.sortWith(_._2.numInFlightReqs < _._2.numInFlightReqs):_*) // OG-AUTOSCALE!

    rankOrderedInvokers.keys.foreach{
      curInvokerId => 
      var curInvokerRunningState = inUseInvokers(curInvokerId)
      var curInvoker = curInvokerRunningState.invkrInstId
      usedInvokers.get(curInvoker) match {
        case Some(curInvokerStats) => 
          logging.info(this,s"\t <adbg> <getAI> action: ${actionName}, invoker: ${curInvoker.toInt} checking whether it has any overloadedCapRemaining...")
          // If I fit, I will choose this.
          // TODO: Change this so that I iterate based on some "ranking"
          val (numInFlightReqs,curAvgLatency,curActOpZone,decision) = curInvokerStats.overloadedCapRemaining(actionName)
          curInvokerRunningState.numInFlightReqs = numInFlightReqs
          curInvokerRunningState.curAvgLatency = curAvgLatency
          curInvokerRunningState.curActOpZone = curActOpZone            

          if(decision){
            logging.info(this,s"\t <adbg> <getAI> Invoker: ${curInvoker.toInt} supposedly has capacity")  

            //logging.info(this,s"\t <adbg> <getAI> Adding Invoker: ${curInvoker.toInt} to inUseInvokers mappings..")  
            numInvokers = inUseInvokers.size 
            // WARNING: Assuming that invokers will go in increasing order..
            if(numInvokers==0) numInvokers=1
            nextInvokerToUse = (curInvokerRunningState.invokerRank+1)%(numInvokers)

            val toUseProactiveInvokerId = (numInvokers%cmplxLastInstUsed.size) //i.e. spawning a proactive container, this is the right candidate..
            logging.info(this,s"\t <adbg> <getAI> Invoker: ${curInvoker.toInt} checking whether toUseProactiveInvokerId: ${toUseProactiveInvokerId} can accommodate new requests.")  
            //val curReqIaspnMetadata = isSquareRootProactiveNeeded(curInvoker,toUseProactiveInvokerId,false,curInvokerProactiveContsToSpawn,numProactiveContsToSpawn); var curAllocMetadata = new allocMetadata(curInvoker,curReqIaspnMetadata)

            //making forcedDummyReq false for no proactiv spawning case
            val curReqIsrpnMetadata = isSquareRootProactiveNeeded(curInvoker,toUseProactiveInvokerId,false,curInvokerProactiveContsToSpawn,numProactiveContsToSpawn)
            //val curReqIsrpnMetadata = isSquareRootProactiveNeeded(curInvoker,toUseProactiveInvokerId,true,curInvokerProactiveContsToSpawn,numProactiveContsToSpawn)
            var curAllocMetadata = new allocMetadata(curInvoker,curReqIsrpnMetadata)
            return Some(curInvoker,curAllocMetadata)//return Some(curInvoker)
          }
        case None =>
          logging.info(this,s"\t <adbg> <getAI> Invoker: ${curInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")
      }
    }
    logging.info(this,s"\t <adbg> <getAI:forced> action: ${actionName} did not get an active invoker. Will force it on leastLoadedInvoker!")

    rankOrderedInvokers.keys.foreach{
      curInvokerId => 
      var curInvokerRunningState = inUseInvokers(curInvokerId)
      var curInvoker = curInvokerRunningState.invkrInstId
      usedInvokers.get(curInvoker) match {
        case Some(curInvokerStats) => 
          logging.info(this,s"\t <adbg> <getAI:forced> action: ${actionName}, invoker: ${curInvoker.toInt} forcing this request on it..")
          // If I fit, I will choose this.
          // TODO: Change this so that I iterate based on some "ranking"
          val (numInFlightReqs,curAvgLatency,curActOpZone,decision) = curInvokerStats.forcedSchedule(actionName) // will always succeed..
          curInvokerRunningState.numInFlightReqs = numInFlightReqs
          curInvokerRunningState.curAvgLatency = curAvgLatency
          curInvokerRunningState.curActOpZone = curActOpZone            

          if(decision){
            logging.info(this,s"\t <adbg> <getAI:forced> Invoker: ${curInvoker.toInt} supposedly has capacity")  

            //logging.info(this,s"\t <adbg> <getAI> Adding Invoker: ${curInvoker.toInt} to inUseInvokers mappings..")  
            numInvokers = inUseInvokers.size 
            // WARNING: Assuming that invokers will go in increasing order..
            if(numInvokers==0) numInvokers=1
            nextInvokerToUse = (curInvokerRunningState.invokerRank+1)%(numInvokers)

            val toUseProactiveInvokerId = (numInvokers%cmplxLastInstUsed.size) //i.e. spawning a proactive container, this is the right candidate..
            logging.info(this,s"\t <adbg> <getAI:forced> Invoker: ${curInvoker.toInt} checking whether toUseProactiveInvokerId: ${toUseProactiveInvokerId} can accommodate new requests.")  
            //val curReqIaspnMetadata = isSquareRootProactiveNeeded(curInvoker,toUseProactiveInvokerId,true,curInvokerProactiveContsToSpawn,numProactiveContsToSpawn); var curAllocMetadata = new allocMetadata(curInvoker,curReqIaspnMetadata)
            val curReqIsrpnMetadata = isSquareRootProactiveNeeded(curInvoker,toUseProactiveInvokerId,true,curInvokerProactiveContsToSpawn,numProactiveContsToSpawn); 
            var curAllocMetadata = new allocMetadata(curInvoker,curReqIsrpnMetadata)

            return Some(curInvoker,curAllocMetadata)//return Some(curInvoker)
          }
        case None =>
          logging.info(this,s"\t <adbg> <getAI:forced> Invoker: ${curInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")
      }
    }
    // if it has come here, then I don't have anything..     
    logging.info(this,s"\t <adbg> <getAI:forced:danger> Couldn't schedule aciton: ${actionName} but why??")
    None
  }

}

class InvokerResources(var numCores: Int, var memorySize: Int){

}

class AdapativeInvokerStats(val id: InvokerInstanceId, val status: InvokerState,logging: Logging) extends functionInfo{
  // begin - copied from InvokerHealth
  override def equals(obj: scala.Any): Boolean = obj match {
    case that: AdapativeInvokerStats => that.id == this.id && that.status == this.status
    case _                   => false
  }

  override def toString = s"AdapativeInvokerStats($id, $status)"
  // end - copied from InvokerHealth
  var myResources = new InvokerResources(4,8*1024) // for now this is by default assumed, should make it parameterized.
  //var numConts = mutable.Map.empty[String,Int] // actionType, numContsOf this action-type
  //numConts = numConts + ("ET" -> 0)
  //numConts = numConts + ("MP" -> 0)
  
  // Max of any action of myType.
  var actionTypeOpZone = mutable.Map.empty[String,Int] // actionType, numContsOf this action-type
  actionTypeOpZone = actionTypeOpZone + ("ET" -> opZoneSafe)
  actionTypeOpZone = actionTypeOpZone + ("MP" -> opZoneSafe)

  var allActions = mutable.Map.empty[String, ActionStatsPerInvoker]
  //var allActionsByType = mutable.Map.empty[String, ListBuffer[String]]
  //allActionsByType = allActionsByType + ("ET" -> new mutable.ListBuffer[String])
  //allActionsByType = allActionsByType + ("MP" -> new mutable.ListBuffer[String])

  var allActionsByType = mutable.Map.empty[String, mutable.Map[String,Int]]
  allActionsByType = allActionsByType + ("ET" -> mutable.Map.empty[String,Int])
  allActionsByType = allActionsByType + ("MP" -> mutable.Map.empty[String,Int])

  var inFlightReqsByType = mutable.Map.empty[String, Int]
  inFlightReqsByType = inFlightReqsByType + ("ET" -> 0)
  inFlightReqsByType = inFlightReqsByType + ("MP" -> 0)

  var inFlightReqsByAct = mutable.Map.empty[String, Int]
  inFlightReqsByAct = inFlightReqsByAct + ("ET" -> 0)
  inFlightReqsByAct = inFlightReqsByAct + ("MP" -> 0)

  var numContsByActType = mutable.Map.empty[String, Int]
  numContsByActType = numContsByActType + ("ET" -> 0)
  numContsByActType = numContsByActType + ("MP" -> 0)

  var maxNumContsByActType = mutable.Map.empty[String, Int]
  maxNumContsByActType = maxNumContsByActType + ("ET" -> 1 * myResources.numCores)
  maxNumContsByActType = maxNumContsByActType + ("MP" -> 1 * myResources.numCores)

  var dummyReqsByAct = mutable.Map.empty[String, Int]
  var accumDummyReqsByAct = mutable.Map.empty[String, Int]
  var actConts = mutable.Map.empty[String, Int]

  var statsUpdatedByAction = mutable.Map.empty[String,Long] // timestamp of last action..

  // Instant.now.toEpochMilli

  var maxInFlightReqsByType = mutable.Map.empty[String, Double]
  maxInFlightReqsByType = maxInFlightReqsByType + ("ET" -> 1.0 * myResources.numCores)
  maxInFlightReqsByType = maxInFlightReqsByType + ("MP" -> 1.0 * myResources.numCores)

  var lastTime_ProactivelySpawned = mutable.Map.empty[String,Long]
  //lastTime_ProactivelySpawned = lastTime_ProactivelySpawned + ("ET" -> 0); //lastTime_ProactivelySpawned = lastTime_ProactivelySpawned + ("MP" -> 0)

  // -------------- Thresholds --------------
  var warningZoneThd:Double = 0.5
  var maxProactiveNumConts: Int = 2
  var curNumProcactiveConts: Int = 0
  //var lastTime_ProactivelySpawned: Long = 0 //Instant.now.toEpochMilli
  // -------------- Thresholds --------------

  def updateInvokerResource(toSetNumCores:Int,toSetMemory: Int): Unit = {
    myResources.numCores = toSetNumCores
    myResources.memorySize = toSetMemory

    maxInFlightReqsByType = maxInFlightReqsByType + ("ET" -> 1.0 * myResources.numCores)
    maxInFlightReqsByType = maxInFlightReqsByType + ("MP" -> 1.0 * myResources.numCores)

    maxNumContsByActType = maxNumContsByActType + ("ET" -> 1 * myResources.numCores)
    maxNumContsByActType = maxNumContsByActType + ("MP" -> 1 * myResources.numCores)

  }

  def addAction(toAddAction: String):Unit = {
    logging.info(this,s"\t <adbg> <AIS:addAction> Trying to add action: ${toAddAction} to allActions")
    allActions.get(toAddAction) match {
      case Some (curActStats) =>
        logging.info(this,s"\t <adbg> <AIS:addAction> invoker: ${id.toInt} Ok action ${toAddAction} IS present in allActions, doing nothing!")
        
      case None => 
        logging.info(this,s"\t <adbg> <AIS:addAction> invoker: ${id.toInt} Ok action ${toAddAction} is NOT present in allActions, adding it..")
        allActions = allActions + (toAddAction -> new ActionStatsPerInvoker(toAddAction,id.toInt,logging))
        var myActType = getActionType(toAddAction)
        // this way, I will only add it once!
        //allActionsByType(myActType)+=toAddAction //
        allActionsByType(myActType) = allActionsByType(myActType) + (toAddAction -> 1 )
        getDummyReqs(toAddAction) 
        getActConts(toAddAction)
        getInFlightReqs(toAddAction)
    }
    var myActType = getActionType(toAddAction);
    var myStandaloneRuntime = getFunctionRuntime(toAddAction)
    logging.info(this,s"\t <adbg> <AIS:addAction> action: ${toAddAction} is of type: ${myActType} and runtime: ${myStandaloneRuntime}")
  }

  def getLastTime_ProactivelySpawned(toCheckAction: String): Long = {
    lastTime_ProactivelySpawned.get(toCheckAction) match {
      case Some(_) =>
      case None =>
        logging.info(this,s"\t <adbg> <gltps-2> invoker: ${id.toInt} act: ${toCheckAction}!")
        lastTime_ProactivelySpawned = lastTime_ProactivelySpawned + (toCheckAction -> 0)
    }
    lastTime_ProactivelySpawned(toCheckAction)
  }  

  def setLastTime_ProactivelySpawned(toCheckAction: String,timeToSet:Long): Unit = {
    lastTime_ProactivelySpawned.get(toCheckAction) match {
      case Some(_) =>
        lastTime_ProactivelySpawned(toCheckAction) = timeToSet
      case None =>
        logging.info(this,s"\t <adbg> <sltps-2> invoker: ${id.toInt} act: ${toCheckAction}!")
        lastTime_ProactivelySpawned = lastTime_ProactivelySpawned + (toCheckAction -> timeToSet)
    }
  }  


  def getActConts(reqAction: String): Int = {
    actConts.get(reqAction) match {
      case Some(_) =>
      case None =>
        logging.info(this,s"\t <adbg> <gdumR-2> invoker: ${id.toInt} act: ${reqAction}!")
        actConts = actConts + (reqAction -> 0)
    }
    actConts(reqAction)
  }

  def updateActConts(reqAction: String, numConts: Int): Unit = {
    var curDummyReqs = getActConts(reqAction)
    actConts(reqAction) = numConts;
    logging.info(this,s"\t <adbg> <updateAC-1> invoker: ${id.toInt} act: ${reqAction} has ${numConts} numConts")    
  }
  
  def getInFlightReqs(toAddAction: String): Int = {
    inFlightReqsByAct.get(toAddAction) match {
      case Some(_) =>
      case None =>
        logging.info(this,s"\t <adbg> <gifR-2> invoker: ${id.toInt} act: ${toAddAction}!")
        inFlightReqsByAct = inFlightReqsByAct + (toAddAction -> 0)
    }
    inFlightReqsByAct(toAddAction)
  }

  def addInFlightReqs(toUpdateAction: String, numReqsToAdd: Int): Unit = {
    var curInFlightReqss = getInFlightReqs(toUpdateAction)
    inFlightReqsByAct(toUpdateAction) = inFlightReqsByAct(toUpdateAction) + numReqsToAdd;
    logging.info(this,s"\t <adbg> <aifr-1> invoker: ${id.toInt} act: ${toUpdateAction} will get ${numReqsToAdd} more requests, making it ${inFlightReqsByAct(toUpdateAction)}")    
  }

  def removeInFlightReqs(toUpdateAction: String,numReqsToRemove: Int): Unit ={
    var curInFlightReqs = getInFlightReqs(toUpdateAction)
    inFlightReqsByAct(toUpdateAction) = inFlightReqsByAct(toUpdateAction) - numReqsToRemove;
    if(inFlightReqsByAct(toUpdateAction)<0)
      inFlightReqsByAct(toUpdateAction) = 0
    logging.info(this,s"\t <adbg> <rifR-1> invoker: ${id.toInt} act: ${toUpdateAction} removing ${numReqsToRemove} more requests, making it ${inFlightReqsByAct(toUpdateAction)} dummy reqs")
  }

  def getDummyReqs(dummyReqAction: String): Int = {
    dummyReqsByAct.get(dummyReqAction) match {
      case Some(_) =>
      case None =>
        logging.info(this,s"\t <adbg> <gdumR-2> invoker: ${id.toInt} act: ${dummyReqAction}!")
        dummyReqsByAct = dummyReqsByAct + (dummyReqAction -> 0)
        accumDummyReqsByAct = accumDummyReqsByAct + (dummyReqAction -> 0)
    }
    dummyReqsByAct(dummyReqAction)
  }

  def getCurActAccumDummyReqs(dummyReqAction: String): Int = {
    dummyReqsByAct.get(dummyReqAction) match {
      case Some(_) =>
        logging.info(this,s"\t <adbg> <gcactadR-1> invoker: ${id.toInt} act: ${dummyReqAction} curDummyReqs: ${dummyReqsByAct(dummyReqAction)} accumDR: ${accumDummyReqsByAct(dummyReqAction) }")
      case None =>
        logging.info(this,s"\t <adbg> <gcactadR-2> invoker: ${id.toInt} act: ${dummyReqAction}!")
        dummyReqsByAct = dummyReqsByAct + (dummyReqAction -> 0)
        accumDummyReqsByAct = accumDummyReqsByAct + (dummyReqAction -> 0)
    }
    accumDummyReqsByAct(dummyReqAction)    
  }
  def addDummyReq(dummyReqAction: String, numReqsToAdd: Int): Unit = {
    var curDummyReqs = getDummyReqs(dummyReqAction)
    dummyReqsByAct(dummyReqAction) = dummyReqsByAct(dummyReqAction) + numReqsToAdd;
    accumDummyReqsByAct(dummyReqAction) = accumDummyReqsByAct(dummyReqAction) + numReqsToAdd
    logging.info(this,s"\t <adbg> <adumR-1> invoker: ${id.toInt} act: ${dummyReqAction} will get ${numReqsToAdd} more requests, making it ${dummyReqsByAct(dummyReqAction)} and accum-dr: ${accumDummyReqsByAct(dummyReqAction)} dummy reqs")    
  }

  def removeDummyReq(dummyReqAction: String,numReqsToRemove: Int): Unit ={
    var curDummyReqs = getDummyReqs(dummyReqAction)
    dummyReqsByAct(dummyReqAction) = dummyReqsByAct(dummyReqAction) - numReqsToRemove;
    logging.info(this,s"\t <adbg> <rdumR-1> invoker: ${id.toInt} act: ${dummyReqAction} removing ${numReqsToRemove} more requests, making it ${dummyReqsByAct(dummyReqAction)} dummy reqs")
  }

  def getMyMaxInFlightReqs(toCheckAction: String): Int= {
    var actType = getActionType(toCheckAction)
    if(actType=="ET"){
      if(inFlightReqsByType("MP")==0){ // Maybe should be number of containers?
        var tempNum:Double = maxInFlightReqsByType("ET")*1.25 // MP : 256 cpu shares of 1/4 cpus; so trying to increase cpu count by 0.25
        tempNum.toInt 
      }else{
        maxInFlightReqsByType("ET").toInt
      }
    }else{
      maxInFlightReqsByType(actType).toInt
    }
  }


  def updateActTypeConts(): Unit ={
    allActionsByType.keys.foreach{
      curActType => 
      //var allActionsOfCurType :ListBuffer[String] = allActionsByType(curActType)
      var allActionsOfCurType :mutable.Map[String,Int]= allActionsByType(curActType)
      var accumNumConts = 0; var maxOpZone = startingOpZone 

      //allActionsOfCurType.foreach{ curAction =>
      allActionsOfCurType.keys.foreach{ curAction =>  
        val numActConts = getActConts(curAction)
        accumNumConts+=numActConts
        //logging.info(this,s"\t <adbg> <AIS:UAC> invoker: ${id.toInt} actType: ${curActType} action: ${curAction}, numConts: ${numActConts} accumNumConts: ${accumNumConts} ")    
      }
      numContsByActType = numContsByActType + (curActType -> accumNumConts)
      logging.info(this,s"\t <adbg> <AIS:UAC> invoker: ${id.toInt} for actType: ${curActType} accumNumConts: ${accumNumConts}")    
    }
  }

  def updateActionStats(toUpdateAction:String, latencyVal: Long, initTime: Long, toUpdateNumConts:Int, toUpdateNumWindowConts: Int):Int = {
    var actType = getActionType(toUpdateAction)
    var bef_pendingReqs = inFlightReqsByType(actType)
    var after_pendingReqs = if(bef_pendingReqs > 0)  bef_pendingReqs-1 else 0
    var curActOpZone = opZoneUnSafe

    if(initTime!=(500*1000)){ // implies regular request..
      inFlightReqsByType(actType) = after_pendingReqs // ok will have an outstanding request of my type..
      removeInFlightReqs(toUpdateAction,1)
    } 
    else{
      removeDummyReq(toUpdateAction,1)
      logging.info(this,s"\t <adbg> <AIS:UAS-0> 1. invoker: ${id.toInt} act: ${toUpdateAction} with latencyVal: ${latencyVal} not updating inFlightReqsByType: ${inFlightReqsByType(actType)} since initTime: ${initTime} matches the default initTime. inFlightReqsByType: ${inFlightReqsByType(actType)} and it's dummyReqs are: ${getDummyReqs(toUpdateAction)}")      
    }

    updateActConts(toUpdateAction,toUpdateNumConts)
    updateActTypeConts()
    allActions.get(toUpdateAction) match {
      case Some(curActStats) => 
        curActStats.update(latencyVal,initTime,toUpdateNumConts,toUpdateNumWindowConts)
        //curActStats.opZoneUpdate()
        curActOpZone = curActStats.opZone
        logging.info(this,s"\t <adbg> <AIS:UAS> 1. invoker: ${id.toInt} bef-pendingReqs: ${bef_pendingReqs} aft-: ${inFlightReqsByType(actType)} action: ${toUpdateAction} opZone: ${curActStats.opZone} numConts: ${curActStats.numConts} numWindowConts: ${curActStats.numWindowConts} movingAvgLatency: ${curActStats.movingAvgLatency} lastUpdated: ${curActStats.lastUpdated}")
      case None =>
        //allActions = allActions + (toUpdateAction -> new ActionStatsPerInvoker(toUpdateAction,logging))
        addAction(toUpdateAction)
        var tempActStats: ActionStatsPerInvoker = allActions(toUpdateAction)
        tempActStats.update(latencyVal,initTime,toUpdateNumConts,toUpdateNumWindowConts)
        curActOpZone = tempActStats.opZone
        logging.info(this,s"\t <adbg> <AIS:UAS> 2. invoker: ${id.toInt} bef-pendingReqs: ${bef_pendingReqs} aft-: ${inFlightReqsByType(actType)} action: ${toUpdateAction} opZone: ${tempActStats.opZone} numConts: ${tempActStats.numConts} numWindowConts: ${tempActStats.numWindowConts} movingAvgLatency: ${tempActStats.movingAvgLatency} lastUpdated: ${tempActStats.lastUpdated}")
    }
    curActOpZone
  }

  def findActionNumContsOpZone(toCheckAction: String): (Int,Int,Long,Long) = {
    logging.info(this,s"\t <adbg> <AIS:FANCO> 0. invoker: ${id.toInt} has action: ${toCheckAction}")             
    allActions.get(toCheckAction) match {
      case Some(curActStats) => 
        var curTime: Long = Instant.now.toEpochMilli
        var timeDiff: Long = curTime - curActStats.lastUpdated

        var resetStatsTimeoutFlag: Boolean = false
        if(curActStats.opZone == opZoneUnSafe)
          resetStatsTimeoutFlag =  timeDiff > curActStats.statsResetTimeout 

        if(  ( timeDiff > statsTimeoutInMilli) || (resetStatsTimeoutFlag) )
          curActStats.resetStats(curTime,resetStatsTimeoutFlag)

        logging.info(this,s"\t <adbg> <AIS:FANCO> 1. invoker: ${id.toInt} has action: ${toCheckAction}, it has numConts: ${curActStats.numConts} and it's opZone: ${curActStats.opZone}")     
        (curActStats.numConts,curActStats.opZone,curActStats.lastUpdated,curActStats.movingAvgLatency)
      case None =>
        //allActions = allActions + (toCheckAction -> new ActionStatsPerInvoker(toCheckAction,id.toInt,logging))
        addAction(toCheckAction)
        logging.info(this,s"\t <adbg> <AIS:FANCO> 2. invoker: ${id.toInt} does NOT have action: ${toCheckAction}.")    
        (0,opZoneSafe,Instant.now.toEpochMilli,0) // FIX-IT: last param is not right..
    }    
  }

  def getActMetadataInInvkr(toCheckAction: String): actMetadataInInvkr = {
    var actType = getActionType(toCheckAction)  
    val (myConts,status_opZone,lastUpdated,actLatency) = findActionNumContsOpZone(toCheckAction)
    var presentActMetadataInInvkr = new actMetadataInInvkr(myConts,status_opZone,inFlightReqsByType(actType),lastUpdated,actLatency)
    presentActMetadataInInvkr
  }

  def updateActTypeStats(): Unit ={
    allActionsByType.keys.foreach{
      curActType => 
      //var allActionsOfCurType :ListBuffer[String] = allActionsByType(curActType)
      var allActionsOfCurType :mutable.Map[String,Int]= allActionsByType(curActType)
      var accumNumConts = 0; var maxOpZone = startingOpZone 

      //allActionsOfCurType.foreach{ curAction =>
      allActionsOfCurType.keys.foreach{ curAction =>  
        val (numConts,thisActOpZone,lastUpdated,actLatency) = findActionNumContsOpZone(curAction)
        
        if(maxOpZone < thisActOpZone)
          maxOpZone = thisActOpZone
        accumNumConts+=numConts
        logging.info(this,s"\t <adbg> <AIS:UATS> invoker: ${id.toInt} actType: ${curActType} action: ${curAction}, numConts: ${numConts} accumNumConts: ${accumNumConts} opZone: ${thisActOpZone}, maxOpZone: ${maxOpZone}")    
      }
      actionTypeOpZone = actionTypeOpZone + (curActType -> maxOpZone)
      numContsByActType = numContsByActType + (curActType -> accumNumConts)
      logging.info(this,s"\t <adbg> <AIS:UATS> invoker: ${id.toInt} for actType: ${curActType} maxOpZone: ${maxOpZone} accumNumConts: ${accumNumConts}")    
    }
  }

  def getCurActNumConts(curActName: String): Int={
    var numActConts = 0
    var thisActOpZone = 0
    var lastUpdated: Long =0
    var actLatency: Long =0

    allActions.get(curActName) match {
      case Some(curActStats) => 
        var curTime: Long = Instant.now.toEpochMilli
        var timeDiff: Long = curTime - curActStats.lastUpdated

        var resetStatsTimeoutFlag: Boolean = false
        if(curActStats.opZone == opZoneUnSafe)
          resetStatsTimeoutFlag =  timeDiff > curActStats.statsResetTimeout 

        if(  ( timeDiff > statsTimeoutInMilli) || (resetStatsTimeoutFlag) )
          curActStats.resetStats(curTime,resetStatsTimeoutFlag)

        logging.info(this,s"\t <adbg> <AIS:gCANC:1> invoker: ${id.toInt} has action: ${curActName}, it has numConts: ${curActStats.numConts} and it's opZone: ${curActStats.opZone}")     
        numActConts = curActStats.numConts
        thisActOpZone = curActStats.opZone 
        lastUpdated = curActStats.lastUpdated 
        actLatency = curActStats.movingAvgLatency
      case None =>
        logging.info(this,s"\t <adbg> <AIS:gCANC:2> invoker: ${id.toInt} does NOT have action: ${curActName}.")    
    }    
    logging.info(this,s"\t <adbg> <AIS:gCANC> 1. invoker: ${id.toInt} action: ${curActName} numActConts: ${numActConts} movingAvgLatency: ${actLatency} thisActOpZone: ${thisActOpZone} lastUpdated: ${lastUpdated}")     
    numActConts
  }

  def getActiveNumConts(): Int = {
    updateActTypeStats() 
    numContsByActType("ET")+inFlightReqsByType("ET")+numContsByActType("MP")+inFlightReqsByType("MP")
  }

  def isInvokerUnsafe(): Boolean = {
    var retVal: Boolean = false
    updateActTypeStats()
    if(actionTypeOpZone("ET")==opZoneUnSafe){
      retVal = true
    }else if(actionTypeOpZone("ET")==opZoneUnSafe){
      retVal = true
    }
    retVal
  }

  def getNumConnections(actionName:String): Int ={
    /*if(getActionType(actionName)=="ET"){
      inFlightReqsByType("ET")
    }else{
      inFlightReqsByType("MP")   
    }*/
    inFlightReqsByType("ET")+inFlightReqsByType("MP")
  }

  def numCoresReqsContsDummyReqsOpZone(actionName: String): (Int,Int,Int,Int,Int,Int,Int) = {
    var actType = getActionType(actionName)
    var (myConts,status_opZone,lastUpdated,actLatency) = findActionNumContsOpZone(actionName)
    (myResources.numCores,inFlightReqsByType(actType),myConts,numContsByActType(actType),maxNumContsByActType(actType),getDummyReqs(actionName),status_opZone)
  }

  def canSquareRootDummyReqBeIssued(actionName:String,curTime:Long):Boolean={
    var timeSinceLastProactive = (curTime - getLastTime_ProactivelySpawned(actionName))
    logging.info(this,s"\t <adbg> <AIS:NPR:END> invoker: ${id.toInt} action: ${actionName} timeSinceLastProactive: ${timeSinceLastProactive}")  
    if(timeSinceLastProactive >= minZoneBaseProactivWaitTime){
      setLastTime_ProactivelySpawned(actionName,curTime)
      true
    }else{
      false
    }
  }

  def issuedAReq(actionName: String, issuedNumDummyReqs: Int): Unit = {
    var actType = getActionType(actionName)
    inFlightReqsByType(actType)+=issuedNumDummyReqs
    if( inFlightReqsByType(actType) > getMyMaxInFlightReqs(actionName) ){
      logging.info(this,s"\t <adbg> <AIS:issuedAReq> 1. DANGER DANGER In invoker-${id.toInt} in pursuit of issuing ${issuedNumDummyReqs} inFlightReqsByType: ${inFlightReqsByType(actType)} are more than the maxInFlightReqs: ${getMyMaxInFlightReqs(actionName)} ")      
    }else{
      logging.info(this,s"\t <adbg> <AIS:issuedAReq> 2. ALL-COOL In invoker-${id.toInt} in pursuit of issuing ${issuedNumDummyReqs} inFlightReqsByType: ${inFlightReqsByType(actType)} is less than maxInFlightReqs: ${getMyMaxInFlightReqs(actionName)} ")      
    }
  }

  def forcedSchedule(actionName:String): (Int,Long,Int,Boolean) = {
    var actType = getActionType(actionName)  
    var otherActType = "ET"
    if(actType=="ET")
      otherActType = "MP"
    else
      otherActType = "ET"

    var retVal: Boolean = true
    var (myConts,status_opZone,lastUpdated,actLatency) = findActionNumContsOpZone(actionName)
    var curInstanceTotalReqs = inFlightReqsByType(actType) 
    var curInstanceMaxReqs =  if(myConts>0) myConts else getMyMaxInFlightReqs(actionName) //maxInFlightReqsByType(actType)  // this is OK because we will call this function only when we are overloaded and we want to ensure we won't spawn new container..

    inFlightReqsByType(actType) = inFlightReqsByType(actType)+1  // ok will have an outstanding request of my type..
    addInFlightReqs(actionName,1)

    logging.info(this,s"\t <adbg> <AIS:forcedCapRem> Final. invoker: ${id.toInt} has action: ${actionName} has curInstanceTotalReqs: ${curInstanceTotalReqs} curInstanceMaxReqs: ${curInstanceMaxReqs} actualMaxReqs: ${getMyMaxInFlightReqs(actionName)} otherActType: ${otherActType} with retVal: ${retVal} and current pendingReqs: ${inFlightReqsByType(actType)} ")
    (inFlightReqsByType(actType),actLatency,status_opZone,retVal)

  }

  def overloadedCapRemaining(actionName:String): (Int,Long,Int,Boolean) = {
    // only called when all the existing invokers are full capacity, so we will choose the least loaded invoker, ignore opzone and send it to the invoker where we won't have to spawn a new invoker!
    var actType = getActionType(actionName)  
    var otherActType = "ET"
    if(actType=="ET")
      otherActType = "MP"
    else
      otherActType = "ET"

    var retVal: Boolean = false
    var (myConts,status_opZone,lastUpdated,actLatency) = findActionNumContsOpZone(actionName)

    var latencyRatio: Double = (actLatency.toDouble/getFunctionRuntime(actionName))
    var toleranceRatio: Double = if(latencyRatio > 1.0) ((latencyRatio-1)/(latencyTolerance-1)) else safeBeginThreshold

    logging.info(this,s"\t <adbg> <AIS:OcapRem> 0. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")
    logging.info(this,s"\t <adbg> <AIS:OcapRem> 2. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")

    var curInstanceTotalReqs = inFlightReqsByType(actType) 
    var curInstanceMaxReqs =  if(myConts>0) myConts else getMyMaxInFlightReqs(actionName) //maxInFlightReqsByType(actType)  // this is OK because we will call this function only when we are overloaded and we want to ensure we won't spawn new container..
    //if ( ( curInstanceTotalReqs < curInstanceMaxReqs) && (status_opZone!= opZoneUnSafe ) ){
    if ( ( curInstanceTotalReqs < curInstanceMaxReqs)){
      logging.info(this,s"\t <adbg> <AIS:OcapRem> myConts: ${myConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
      // ok, I don't have too many pending requests here..
      retVal = true
    }else{
      logging.info(this,s"\t <adbg> <AIS:OcapRem> invoker: ${id.toInt} curInstanceMaxReqs(actType): ${curInstanceMaxReqs} numCores: ${myResources.numCores} status_opZone: ${status_opZone} myConts: ${myConts} ")
      // No way JOSE!
      retVal = false
    }
    if(retVal) {
      inFlightReqsByType(actType) = inFlightReqsByType(actType)+1  // ok will have an outstanding request of my type..
      addInFlightReqs(actionName,1)
    }

    logging.info(this,s"\t <adbg> <AIS:OcapRem> Final. invoker: ${id.toInt} has action: ${actionName} has curInstanceTotalReqs: ${curInstanceTotalReqs} curInstanceMaxReqs: ${curInstanceMaxReqs} actualMaxReqs: ${getMyMaxInFlightReqs(actionName)} otherActType: ${otherActType} with retVal: ${retVal} and current pendingReqs: ${inFlightReqsByType(actType)} ")
    logging.info(this,s"\t <adbg> <OCapRem:final> ${id.toInt} ${actionName} ${actLatency} ${f"$toleranceRatio%1.3f"} ${status_opZone} reqs: ${curInstanceTotalReqs} ${curInstanceMaxReqs} ${inFlightReqsByType(otherActType)} retVal: ${retVal} ")
    (inFlightReqsByType(actType),actLatency,status_opZone,retVal)
  }

  def capacityRemaining(actionName:String): (Int,Long,Int,Boolean) = { // should update based on -- memory; #et, #mp and operating zone
    // 1. Check action-type. Alternatively, can send this as a parameter from the schedule-method
    // 2. Check whether we can accommodate this actionType (ET vs MP)? 
    //  2.a. If already a container of this action exists, ensure it is in safe opZone.
    //  2.b. If a container of this action doesn't exist, check whether we can add another container of this actionType (i.e. Check whether there are enough-cores available)
    //  Current Assumption: Ok to add a new-action, as long as all acitons are not in unsafe-region.
    
    var actType = getActionType(actionName)  
    var otherActType = "ET"
    if(actType=="ET")
      otherActType = "MP"
    else
      otherActType = "ET"

    var retVal: Boolean = false
    var (myConts,status_opZone,lastUpdated,actLatency) = findActionNumContsOpZone(actionName)

    var latencyRatio: Double = (actLatency.toDouble/getFunctionRuntime(actionName))
    var toleranceRatio: Double = if(latencyRatio > 1.0) ((latencyRatio-1)/(latencyTolerance-1)) else safeBeginThreshold

    logging.info(this,s"\t <adbg> <AIS:capRem> 0. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")
    logging.info(this,s"\t <adbg> <AIS:capRem> 2. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")

    var curInstanceMaxReqs =  getMyMaxInFlightReqs(actionName) //maxInFlightReqsByType(actType) 
    var curInstanceTotalReqs = inFlightReqsByType(actType) 

    var curActNumConts = getActConts(actionName)
    var curActTypeConts = numContsByActType(actType)
    var curActTypeMaxConts = maxNumContsByActType(actType)
    var curActionReqs = getInFlightReqs(actionName)
    /*//Used for ICDCS submission 
    if( (status_opZone == opZoneWarn) || (status_opZone == opZoneUndetermined) ){ // opZoneWarn ==> Round-Robin-Mode
      //curInstanceMaxReqs =  0.75 * maxInFlightReqsByType(actType) 
      curInstanceMaxReqs =  1 // use RR when it is in status equal or higher in Warning zone..
    }*/

    if( (status_opZone == opZoneWarn) || (status_opZone == opZoneUndetermined)  ){ // opZoneWarn ==> Round-Robin-Mode
      curInstanceMaxReqs =  1 // use RR when it is in status equal or higher in Warning zone..
    }else{
      if(curInstanceMaxReqs > myConts)
        curInstanceMaxReqs = myConts
    }

    var curActionConts = 0
    if(myConts==0) curActionConts = 1
    //if ( (curActionReqs < curActionConts) && ( curInstanceTotalReqs < curInstanceMaxReqs) && (status_opZone!= opZoneUnSafe ) ){
    if ( ( curInstanceTotalReqs < curInstanceMaxReqs) && (status_opZone!= opZoneUnSafe ) ){
      logging.info(this,s"\t <adbg> <AIS:capRem> myConts: ${myConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
      // ok, I don't have too many pending requests here..
      retVal = true
    }else{
      logging.info(this,s"\t <adbg> <AIS:capRem> invoker: ${id.toInt} curInstanceMaxReqs(actType): ${curInstanceMaxReqs} numCores: ${myResources.numCores} status_opZone: ${status_opZone} myConts: ${myConts} ")
      // No way JOSE!
      retVal = false
    }
    if(retVal) {
      inFlightReqsByType(actType) = inFlightReqsByType(actType)+1  // ok will have an outstanding request of my type..
      addInFlightReqs(actionName,1)
    }


    logging.info(this,s"\t <adbg> <AIS:capRem> Final. invoker: ${id.toInt} has action: ${actionName} has curInstanceTotalReqs: ${curInstanceTotalReqs} curActTypeConts: ${curActTypeConts} actTypeMaxConts: ${curActTypeMaxConts} curInstanceMaxReqs: ${curInstanceMaxReqs} actualMaxReqs: ${getMyMaxInFlightReqs(actionName)} otherActType: ${otherActType} with retVal: ${retVal} and current pendingReqs: ${inFlightReqsByType(actType)} ")
    logging.info(this,s"\t <adbg> <capRem:final> ${id.toInt} ${actionName} ${actLatency} ${f"$toleranceRatio%1.3f"} ${status_opZone} reqs: ${curActionReqs} ${curInstanceTotalReqs} ${curInstanceMaxReqs} ${inFlightReqsByType(otherActType)} retVal: ${retVal} ")
    (inFlightReqsByType(actType),actLatency,status_opZone,retVal)
  }

}

class uselessMetadata(val id: InvokerInstanceId, val decision:Boolean) {
  // Will be used to send data back and forth!
}

//class allocMetadata(val allocInvokerId:InvokerInstanceId,val curReqIaspnMetadata:ipinMetadata){}

class ipinMetadata(val likelyProactiveInvoker:InvokerInstanceId, val numProactiveReqs:Int, val numContsToSpawnInMyInvoker:Int,val needToSpawnProactiveInvoker:Boolean) {
  // Will be used to send data back and forth!
}

class allocMetadata(val allocInvokerId:InvokerInstanceId,val curReqIsrpnMetadata:isrpnMetadata){}
class isrpnMetadata(val proactivInvkr1:InvokerInstanceId,val proactivInvkr1NumReqs:Int,val proactivInvkr2:InvokerInstanceId,val proactivInvkr2NumReqs:Int){
}

class actMetadataInInvkr(val numConts: Int,val thisActOpZone:Int ,val numInFlightReqs:Int,val lastUpdated:Long,val actLatency:Long){

}

// avs --end

/**
 * Describes an abstract invoker. An invoker is a local container pool manager that
 * is in charge of the container life cycle management.
 *
 * @param id a unique instance identifier for the invoker
 * @param status it status (healthy, unhealthy, offline)
 */
class InvokerHealth(val id: InvokerInstanceId, val status: InvokerState) {
  //var myStats: AdapativeInvokerStats = new AdapativeInvokerStats(id,status) // avs
  override def equals(obj: scala.Any): Boolean = obj match {
    case that: InvokerHealth => that.id == this.id && that.status == this.status
    case _                   => false
  }

  override def toString = s"InvokerHealth($id, $status)"
}

trait LoadBalancer {

  /**
   * Publishes activation message on internal bus for an invoker to pick up.
   *
   * @param action the action to invoke
   * @param msg the activation message to publish on an invoker topic
   * @param transid the transaction id for the request
   * @return result a nested Future the outer indicating completion of publishing and
   *         the inner the completion of the action (i.e., the result)
   *         if it is ready before timeout (Right) otherwise the activation id (Left).
   *         The future is guaranteed to complete within the declared action time limit
   *         plus a grace period (see activeAckTimeoutGrace).
   */
  def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]]

  /**
   * Returns a message indicating the health of the containers and/or container pool in general.
   *
   * @return a Future[IndexedSeq[InvokerHealth]] representing the health of the pools managed by the loadbalancer.
   */
  def invokerHealth(): Future[IndexedSeq[InvokerHealth]]

  /** Gets the number of in-flight activations for a specific user. */
  def activeActivationsFor(namespace: UUID): Future[Int]

  /** Gets the number of in-flight activations in the system. */
  def totalActiveActivations: Future[Int]

  /** Gets the size of the cluster all loadbalancers are acting in */
  def clusterSize: Int = 1
}

/**
 * An Spi for providing load balancer implementations.
 */
trait LoadBalancerProvider extends Spi {
  def requiredProperties: Map[String, String]

  def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                         logging: Logging,
                                                                         materializer: ActorMaterializer): LoadBalancer

  /** Return default FeedFactory */
  def createFeedFactory(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): FeedFactory = {

    val activeAckTopic = s"completed${instance.asString}"
    val maxActiveAcksPerPoll = 256 // 128
    val activeAckPollDuration = 250.milliseconds //1.second

    new FeedFactory {
      def createFeed(f: ActorRefFactory, provider: MessagingProvider, acker: Array[Byte] => Future[Unit]) = {
        f.actorOf(Props {
          new MessageFeed(
            "activeack",
            logging,
            provider.getConsumer(whiskConfig, activeAckTopic, activeAckTopic, maxPeek = maxActiveAcksPerPoll),
            maxActiveAcksPerPoll,
            activeAckPollDuration,
            acker)
        })
      }
    }
  }
// avs --begin
  def createLoadFeedFactory(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): FeedFactory = {

    val activeAckTopic = s"load-completed${instance.asString}"
    val maxActiveAcksPerPoll = 256 // 128
    val activeAckPollDuration = 250.milliseconds //1.second

    new FeedFactory {
      def createFeed(f: ActorRefFactory, provider: MessagingProvider, acker: Array[Byte] => Future[Unit]) = {
        f.actorOf(Props {
          new MessageFeed(
            "loadResponse",
            logging,
            provider.getConsumer(whiskConfig, activeAckTopic, activeAckTopic, maxPeek = maxActiveAcksPerPoll),
            maxActiveAcksPerPoll,
            activeAckPollDuration,
            acker)
        })
      }
    }
  }
// avs --end  
}

/** Exception thrown by the loadbalancer */
case class LoadBalancerException(msg: String) extends Throwable(msg)
