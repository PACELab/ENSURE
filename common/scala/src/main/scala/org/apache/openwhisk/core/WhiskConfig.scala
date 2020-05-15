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

package org.apache.openwhisk.core

import java.io.File

import akka.http.scaladsl.model.Uri.normalize
import org.apache.openwhisk.common.{Config, Logging}

import scala.io.Source
import scala.util.Try

/**
 * A set of properties which might be needed to run a whisk microservice implemented
 * in scala.
 *
 * @param requiredProperties a Map whose keys define properties that must be bound to
 *                           a value, and whose values are default values. A null value in the Map means there is
 *                           no default value specified, so it must appear in the properties file.
 * @param optionalProperties a set of optional properties (which may not be defined).
 * @param propertiesFile     a File object, the whisk.properties file, which if given contains the property values.
 * @param env                an optional environment to initialize from.
 */
class WhiskConfig(requiredProperties: Map[String, String],
                  optionalProperties: Set[String] = Set.empty,
                  propertiesFile: File = null,
                  env: Map[String, String] = sys.env)(implicit logging: Logging)
    extends Config(requiredProperties, optionalProperties)(env) {

  /**
   * Loads the properties as specified above.
   *
   * @return a pair which is the Map defining the properties, and a boolean indicating whether validation succeeded.
   */
  override protected def getProperties() = {
    val properties = super.getProperties()
    WhiskConfig.readPropertiesFromFile(properties, Option(propertiesFile) getOrElse (WhiskConfig.whiskPropertiesFile))
    properties
  }

  val servicePort = this(WhiskConfig.servicePort)
  val dockerEndpoint = this(WhiskConfig.dockerEndpoint)
  val dockerPort = this(WhiskConfig.dockerPort)

  val wskApiHost: String = Try(
    normalize(
      s"${this(WhiskConfig.wskApiProtocol)}://${this(WhiskConfig.wskApiHostname)}:${this(WhiskConfig.wskApiPort)}"))
    .getOrElse("")

  val controllerBlackboxFraction = this.getAsDouble(WhiskConfig.controllerBlackboxFraction, 0.10)
  val controllerInstances = this(WhiskConfig.controllerInstances)

  val edgeHost = this(WhiskConfig.edgeHostName) + ":" + this(WhiskConfig.edgeHostApiPort)
  val kafkaHosts = this(WhiskConfig.kafkaHostList)

  val edgeHostName = this(WhiskConfig.edgeHostName)

  val invokerHosts = this(WhiskConfig.invokerHostsList)
  val zookeeperHosts = this(WhiskConfig.zookeeperHostList)

  val dbPrefix = this(WhiskConfig.dbPrefix)
  val mainDockerEndpoint = this(WhiskConfig.mainDockerEndpoint)

  val runtimesManifest = this(WhiskConfig.runtimesManifest)
  val actionInvokePerMinuteLimit = this(WhiskConfig.actionInvokePerMinuteLimit)
  val actionInvokeConcurrentLimit = this(WhiskConfig.actionInvokeConcurrentLimit)
  val triggerFirePerMinuteLimit = this(WhiskConfig.triggerFirePerMinuteLimit)
  val actionSequenceLimit = this(WhiskConfig.actionSequenceMaxLimit)
  val controllerSeedNodes = this(WhiskConfig.controllerSeedNodes)
}

object WhiskConfig {

  /**
   * Reads a key from system environment as if it was part of WhiskConfig.
   */
  def readFromEnv(key: String): Option[String] = sys.env.get(asEnvVar(key))

  private def whiskPropertiesFile: File = {
    def propfile(dir: String, recurse: Boolean = false): File =
      if (dir != null) {
        val base = new File(dir)
        val file = new File(base, "whisk.properties")
        if (file.exists())
          file
        else if (recurse)
          propfile(base.getParent, true)
        else null
      } else null

    val dir = sys.props.get("user.dir")
    if (dir.isDefined) {
      propfile(dir.get, true)
    } else {
      null
    }
  }

  /**
   * Reads a Map of key-value pairs from the environment (sys.env) -- store them in the
   * mutable properties object.
   */
  def readPropertiesFromFile(properties: scala.collection.mutable.Map[String, String], file: File)(
    implicit logging: Logging) = {
    if (file != null && file.exists) {
      logging.info(this, s"reading properties from file $file")
      val source = Source.fromFile(file)
      try {
        for (line <- source.getLines if line.trim != "") {
          val parts = line.split('=')
          if (parts.length >= 1) {
            val p = parts(0).trim
            val v = if (parts.length == 2) parts(1).trim else ""
            if (properties.contains(p)) {
              properties += p -> v
              logging.debug(this, s"properties file set value for $p")
            }
          } else {
            logging.warn(this, s"ignoring properties $line")
          }
        }
      } finally {
        source.close()
      }
    }
  }

  def asEnvVar(key: String): String = {
    if (key != null)
      key.replace('.', '_').toUpperCase
    else null
  }

  val servicePort = "port"
  val dockerPort = "docker.port"

  val dockerEndpoint = "main.docker.endpoint"
  val dbPrefix = "db.prefix"
  // these are not private because they are needed
  // in the invoker (they are part of the environment
  // passed to the user container)
  val edgeHostName = "edge.host"

  val wskApiProtocol = "whisk.api.host.proto"
  val wskApiPort = "whisk.api.host.port"
  val wskApiHostname = "whisk.api.host.name"
  val wskApiHost = Map(wskApiProtocol -> "https", wskApiPort -> 443.toString, wskApiHostname -> null)

  val mainDockerEndpoint = "main.docker.endpoint"

  val controllerBlackboxFraction = "controller.blackboxFraction"
  val controllerInstances = "controller.instances"
  val dbInstances = "db.instances"

  val kafkaHostList = "kafka.hosts"
  val zookeeperHostList = "zookeeper.hosts"

  private val edgeHostApiPort = "edge.host.apiport"

  val invokerHostsList = "invoker.hosts"
  val dbHostsList = "db.hostsList"

  val edgeHost = Map(edgeHostName -> null, edgeHostApiPort -> null)
  val invokerHosts = Map(invokerHostsList -> null)
  val kafkaHosts = Map(kafkaHostList -> null)
  val zookeeperHosts = Map(zookeeperHostList -> null)

  val runtimesManifest = "runtimes.manifest"

  val actionSequenceMaxLimit = "limits.actions.sequence.maxLength"
  val actionInvokePerMinuteLimit = "limits.actions.invokes.perMinute"
  val actionInvokeConcurrentLimit = "limits.actions.invokes.concurrent"
  val triggerFirePerMinuteLimit = "limits.triggers.fires.perMinute"
  val controllerSeedNodes = "akka.cluster.seed.nodes"
}

object ConfigKeys {
  val cluster = "whisk.cluster"
  val loadbalancer = "whisk.loadbalancer"
  val buildInformation = "whisk.info"

  val couchdb = "whisk.couchdb"
  val cosmosdb = "whisk.cosmosdb"
  val kafka = "whisk.kafka"
  val kafkaCommon = s"$kafka.common"
  val kafkaProducer = s"$kafka.producer"
  val kafkaConsumer = s"$kafka.consumer"
  val kafkaTopics = s"$kafka.topics"

  val memory = "whisk.memory"
  val timeLimit = "whisk.time-limit"
  val logLimit = "whisk.log-limit"
  val concurrencyLimit = "whisk.concurrency-limit"
  val inferredLimit = "whisk.inferred-limit" //avs
  val activation = "whisk.activation"
  val userEvents = "whisk.user-events"

  val runtimes = "whisk.runtimes"
  val runtimesWhitelists = s"$runtimes.whitelists"

  val db = "whisk.db"

  val docker = "whisk.docker"
  val dockerClient = s"$docker.client"
  val dockerContainerFactory = s"$docker.container-factory"
  val runc = "whisk.runc"
  val runcTimeouts = s"$runc.timeouts"

  val tracing = "whisk.tracing"

  val containerFactory = "whisk.container-factory"
  val containerArgs = s"$containerFactory.container-args"
  val runtimesRegistry = s"$containerFactory.runtimes-registry"
  val containerPool = "whisk.container-pool"
  val blacklist = "whisk.blacklist"

  val kubernetes = "whisk.kubernetes"
  val kubernetesTimeouts = s"$kubernetes.timeouts"

  val transactions = "whisk.transactions"

  val logStore = "whisk.logstore"
  val splunk = s"$logStore.splunk"
  val logStoreElasticSearch = s"$logStore.elasticsearch"

  val mesos = "whisk.mesos"
  val yarn = "whisk.yarn"

  val containerProxy = "whisk.container-proxy"
  val containerProxyTimeouts = s"$containerProxy.timeouts"

  val s3 = "whisk.s3"
  val query = "whisk.query-limit"
  val execSizeLimit = "whisk.exec-size-limit"

  val controller = s"whisk.controller"
  val controllerActivation = s"$controller.activation"

  val activationStore = "whisk.activation-store"
  val activationStoreWithFileStorage = s"$activationStore.with-file-storage"

  val metrics = "whisk.metrics"
  val featureFlags = "whisk.feature-flags"
}
