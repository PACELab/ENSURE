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

package org.apache.openwhisk.core.database

import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.EnumSet

import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.scaladsl.{Flow, MergeHub, RestartSink, Sink, Source}
import akka.util.ByteString
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.containerpool.logging.ElasticSearchJsonProtocol._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import spray.json._

import scala.concurrent.duration._

class ActivationFileStorage(logFilePrefix: String,
                            logPath: Path,
                            writeResultToFile: Boolean,
                            actorMaterializer: ActorMaterializer,
                            logging: Logging) {

  implicit val materializer = actorMaterializer

  private var logFile = logPath
  private val bufferSize = 100.MB
  private val perms = EnumSet.of(OWNER_READ, OWNER_WRITE, GROUP_READ, GROUP_WRITE, OTHERS_READ, OTHERS_WRITE)
  private val writeToFile: Sink[ByteString, _] = MergeHub
    .source[ByteString]
    .batchWeighted(bufferSize.toBytes, _.length, identity)(_ ++ _)
    .to(RestartSink.withBackoff(minBackoff = 1.seconds, maxBackoff = 60.seconds, randomFactor = 0.2) { () =>
      LogRotatorSink(() => {
        val maxSize = bufferSize.toBytes
        var bytesRead = maxSize
        element =>
          {
            val size = element.size

            if (bytesRead + size > maxSize) {
              logFile = logPath.resolve(s"$logFilePrefix-${Instant.now.toEpochMilli}.log")

              logging.info(this, s"Rotating log file to '$logFile'")
              createLogFile(logFile)
              bytesRead = size
              Some(logFile)
            } else {
              bytesRead += size
              None
            }
          }
      })
    })
    .run()

  private def createLogFile(path: Path) =
    try {
      Files.createFile(path)
      Files.setPosixFilePermissions(path, perms)
    } catch {
      case t: Throwable =>
        logging.error(this, s"Couldn't create user log file '$t'")
        throw t
    }

  private def transcribeLogs(activation: WhiskActivation, additionalFields: Map[String, JsValue]) =
    activation.logs.logs.map { log =>
      val line = JsObject(
        Map("type" -> "user_log".toJson) ++ Map("message" -> log.toJson) ++ Map(
          "activationId" -> activation.activationId.toJson) ++ additionalFields)

      ByteString(s"${line.compactPrint}\n")
    }

  private def transcribeActivation(activation: WhiskActivation, additionalFields: Map[String, JsValue]) = {
    val transactionType = Map("type" -> "activation_record".toJson)
    val message = Map(if (writeResultToFile) {
      "message" -> JsString(activation.response.result.getOrElse(JsNull).compactPrint)
    } else {
      "message" -> JsString(s"Activation record '${activation.activationId}' for entity '${activation.name}'")
    })
    val annotations = activation.annotations.toJsObject.fields
    val addFields = transactionType ++ annotations ++ message ++ additionalFields
    val removeFields = Seq("logs", "annotations")
    val line = activation.metadata.toExtendedJson(removeFields, addFields)

    ByteString(s"${line.compactPrint}\n")
  }

  def getLogFile = logFile

  def activationToFile(activation: WhiskActivation,
                       context: UserContext,
                       additionalFields: Map[String, JsValue] = Map.empty) = {
    val transcribedLogs = transcribeLogs(activation, additionalFields)
    val transcribedActivation = transcribeActivation(activation, additionalFields)

    // Write each log line to file and then write the activation metadata
    Source
      .fromIterator(() => transcribedLogs.toIterator)
      .runWith(Flow[ByteString].concat(Source.single(transcribedActivation)).to(writeToFile))
  }
}
