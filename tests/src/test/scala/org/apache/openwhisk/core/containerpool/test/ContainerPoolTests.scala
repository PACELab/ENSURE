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

package org.apache.openwhisk.core.containerpool.test

import java.time.Instant

import scala.collection.mutable
import scala.concurrent.duration._
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.testkit.TestProbe
import common.WhiskProperties
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.ExecManifest.RuntimeManifest
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.connector.MessageFeed

/**
 * Behavior tests for the ContainerPool
 *
 * These tests test the runtime behavior of a ContainerPool actor.
 */
@RunWith(classOf[JUnitRunner])
class ContainerPoolTests
    extends TestKit(ActorSystem("ContainerPool"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory {

  override def afterAll = TestKit.shutdownActorSystem(system)

  val timeout = 5.seconds

  // Common entities to pass to the tests. We don't really care what's inside
  // those for the behavior testing here, as none of the contents will really
  // reach a container anyway. We merely assert that passing and extraction of
  // the values is done properly.
  val exec = CodeExecAsString(RuntimeManifest("actionKind", ImageName("testImage")), "testCode", None)
  val memoryLimit = 256.MB

  /** Creates a `Run` message */
  def createRunMessage(action: ExecutableWhiskAction, invocationNamespace: EntityName) = {
    val uuid = UUID()
    val message = ActivationMessage(
      TransactionId.testing,
      action.fullyQualifiedName(true),
      action.rev,
      Identity(Subject(), Namespace(invocationNamespace, uuid), BasicAuthenticationAuthKey(uuid, Secret()), Set.empty),
      ActivationId.generate(),
      ControllerInstanceId("0"),
      blocking = false,
      content = None)
    Run(action, message)
  }

  val invocationNamespace = EntityName("invocationSpace")
  val differentInvocationNamespace = EntityName("invocationSpace2")
  val action = ExecutableWhiskAction(EntityPath("actionSpace"), EntityName("actionName"), exec)
  val concurrencyEnabled = Option(WhiskProperties.getProperty("whisk.action.concurrency")).exists(_.toBoolean)
  val concurrentAction = ExecutableWhiskAction(
    EntityPath("actionSpace"),
    EntityName("actionName"),
    exec,
    limits = ActionLimits(concurrency = ConcurrencyLimit(if (concurrencyEnabled) 3 else 1)))
  val differentAction = action.copy(name = EntityName("actionName2"))
  val largeAction =
    action.copy(
      name = EntityName("largeAction"),
      limits = ActionLimits(memory = MemoryLimit(MemoryLimit.stdMemory * 2)))

  val runMessage = createRunMessage(action, invocationNamespace)
  val runMessageLarge = createRunMessage(largeAction, invocationNamespace)
  val runMessageDifferentAction = createRunMessage(differentAction, invocationNamespace)
  val runMessageDifferentVersion = createRunMessage(action.copy().revision(DocRevision("v2")), invocationNamespace)
  val runMessageDifferentNamespace = createRunMessage(action, differentInvocationNamespace)
  val runMessageDifferentEverything = createRunMessage(differentAction, differentInvocationNamespace)
  val runMessageConcurrent = createRunMessage(concurrentAction, invocationNamespace)
  val runMessageConcurrentDifferentNamespace = createRunMessage(concurrentAction, differentInvocationNamespace)

  /** Helper to create PreWarmedData */
  def preWarmedData(kind: String, memoryLimit: ByteSize = memoryLimit) =
    PreWarmedData(stub[Container], kind, memoryLimit)

  /** Helper to create WarmedData */
  def warmedData(action: ExecutableWhiskAction = action,
                 namespace: String = "invocationSpace",
                 lastUsed: Instant = Instant.now) =
    WarmedData(stub[Container], EntityName(namespace), action, lastUsed)

  /** Creates a sequence of containers and a factory returning this sequence. */
  def testContainers(n: Int) = {
    val containers = (0 to n).map(_ => TestProbe())
    val queue = mutable.Queue(containers: _*)
    val factory = (fac: ActorRefFactory) => queue.dequeue().ref
    (containers, factory)
  }

  def poolConfig(userMemory: ByteSize) = ContainerPoolConfig(userMemory, 0.5, false)

  behavior of "ContainerPool"

  /*
   * CONTAINER SCHEDULING
   *
   * These tests only test the simplest approaches. Look below for full coverage tests
   * of the respective scheduling methods.
   */
  it should "reuse a warm container" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()
    // Actions are created with default memory limit (MemoryLimit.stdMemory). This means 4 actions can be scheduled.
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 4), feed.ref))

    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, NeedWork(warmedData()))

    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(1).expectNoMessage(100.milliseconds)
  }

  it should "reuse a warm container when action is the same even if revision changes" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()
    // Actions are created with default memory limit (MemoryLimit.stdMemory). This means 4 actions can be scheduled.
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 4), feed.ref))

    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, NeedWork(warmedData()))

    pool ! runMessageDifferentVersion
    containers(0).expectMsg(runMessageDifferentVersion)
    containers(1).expectNoMessage(100.milliseconds)
  }

  it should "create a container if it cannot find a matching container" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    // Actions are created with default memory limit (MemoryLimit.stdMemory). This means 4 actions can be scheduled.
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 4), feed.ref))
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    // Note that the container doesn't respond, thus it's not free to take work
    pool ! runMessage
    containers(1).expectMsg(runMessage)
  }

  it should "remove a container to make space in the pool if it is already full and a different action arrives" in within(
    timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    // a pool with only 1 slot
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory), feed.ref))
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
    pool ! runMessageDifferentEverything
    containers(0).expectMsg(Remove)
    containers(1).expectMsg(runMessageDifferentEverything)
  }

  it should "remove several containers to make space in the pool if it is already full and a different large action arrives" in within(
    timeout) {
    val (containers, factory) = testContainers(3)
    val feed = TestProbe()

    // a pool with slots for 2 actions with default memory limit.
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(512.MB), feed.ref))
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    pool ! runMessageDifferentAction // 2 * stdMemory taken -> full
    containers(1).expectMsg(runMessageDifferentAction)

    containers(0).send(pool, NeedWork(warmedData())) // first action finished -> 1 * stdMemory taken
    feed.expectMsg(MessageFeed.Processed)
    containers(1).send(pool, NeedWork(warmedData())) // second action finished -> 1 * stdMemory taken
    feed.expectMsg(MessageFeed.Processed)

    pool ! runMessageLarge // need to remove both action to make space for the large action (needs 2 * stdMemory)
    containers(0).expectMsg(Remove)
    containers(1).expectMsg(Remove)
    containers(2).expectMsg(runMessageLarge)
  }

  it should "cache a container if there is still space in the pool" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    // a pool with only 1 active slot but 2 slots in total
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 2), feed.ref))

    // Run the first container
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, NeedWork(warmedData(lastUsed = Instant.EPOCH)))
    feed.expectMsg(MessageFeed.Processed)

    // Run the second container, don't remove the first one
    pool ! runMessageDifferentEverything
    containers(1).expectMsg(runMessageDifferentEverything)
    containers(1).send(pool, NeedWork(warmedData(lastUsed = Instant.now)))
    feed.expectMsg(MessageFeed.Processed)
    pool ! runMessageDifferentNamespace
    containers(2).expectMsg(runMessageDifferentNamespace)

    // 2 Slots exhausted, remove the first container to make space
    containers(0).expectMsg(Remove)
  }

  it should "remove a container to make space in the pool if it is already full and another action with different invocation namespace arrives" in within(
    timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    // a pool with only 1 slot
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory), feed.ref))
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
    pool ! runMessageDifferentNamespace
    containers(0).expectMsg(Remove)
    containers(1).expectMsg(runMessageDifferentNamespace)
  }

  it should "reschedule job when container is removed prematurely without sending message to feed" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    // a pool with only 1 slot
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory), feed.ref))
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, RescheduleJob) // emulate container failure ...
    containers(0).send(pool, runMessage) // ... causing job to be rescheduled
    feed.expectNoMessage(100.millis)
    containers(1).expectMsg(runMessage) // job resent to new actor
  }

  it should "not start a new container if there is not enough space in the pool" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 2), feed.ref))

    // Start first action
    pool ! runMessage // 1 * stdMemory taken
    containers(0).expectMsg(runMessage)

    // Send second action to the pool
    pool ! runMessageLarge // message is too large to be processed immediately.
    containers(1).expectNoMessage(100.milliseconds)

    // First action is finished
    containers(0).send(pool, NeedWork(warmedData())) // pool is empty again.
    feed.expectMsg(MessageFeed.Processed)

    // Second action should run now
    containers(1).expectMsgPF() {
      // The `Some` assures, that it has been retried while the first action was still blocking the invoker.
      case Run(runMessageLarge.action, runMessageLarge.msg, Some(_)) => true
    }

    containers(1).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
  }

  /*
   * CONTAINER PREWARMING
   */
  it should "create prewarmed containers on startup" in within(timeout) {
    val (containers, factory) = testContainers(1)
    val feed = TestProbe()

    val pool =
      system.actorOf(
        ContainerPool
          .props(factory, poolConfig(0.MB), feed.ref, List(PrewarmingConfig(1, exec, memoryLimit))))
    containers(0).expectMsg(Start(exec, memoryLimit))
  }

  it should "use a prewarmed container and create a new one to fill its place" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val pool =
      system.actorOf(
        ContainerPool
          .props(factory, poolConfig(MemoryLimit.stdMemory), feed.ref, List(PrewarmingConfig(1, exec, memoryLimit))))
    containers(0).expectMsg(Start(exec, memoryLimit))
    containers(0).send(pool, NeedWork(preWarmedData(exec.kind)))
    pool ! runMessage
    containers(1).expectMsg(Start(exec, memoryLimit))
  }

  it should "not use a prewarmed container if it doesn't fit the kind" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val alternativeExec = CodeExecAsString(RuntimeManifest("anotherKind", ImageName("testImage")), "testCode", None)

    val pool = system.actorOf(
      ContainerPool
        .props(
          factory,
          poolConfig(MemoryLimit.stdMemory),
          feed.ref,
          List(PrewarmingConfig(1, alternativeExec, memoryLimit))))
    containers(0).expectMsg(Start(alternativeExec, memoryLimit)) // container0 was prewarmed
    containers(0).send(pool, NeedWork(preWarmedData(alternativeExec.kind)))
    pool ! runMessage
    containers(1).expectMsg(runMessage) // but container1 is used
  }

  it should "not use a prewarmed container if it doesn't fit memory wise" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val alternativeLimit = 128.MB

    val pool =
      system.actorOf(ContainerPool
        .props(factory, poolConfig(MemoryLimit.stdMemory), feed.ref, List(PrewarmingConfig(1, exec, alternativeLimit))))
    containers(0).expectMsg(Start(exec, alternativeLimit)) // container0 was prewarmed
    containers(0).send(pool, NeedWork(preWarmedData(exec.kind, alternativeLimit)))
    pool ! runMessage
    containers(1).expectMsg(runMessage) // but container1 is used
  }

  /*
   * CONTAINER DELETION
   */
  it should "not reuse a container which is scheduled for deletion" in within(timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 4), feed.ref))

    // container0 is created and used
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, NeedWork(warmedData()))

    // container0 is reused
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(0).send(pool, NeedWork(warmedData()))

    // container0 is deleted
    containers(0).send(pool, ContainerRemoved)

    // container1 is created and used
    pool ! runMessage
    containers(1).expectMsg(runMessage)
  }

  /*
   * Run buffer
   */
  it should "first put messages into the queue and retrying them and then put messages only into the queue" in within(
    timeout) {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    // Pool with 512 MB usermemory
    val pool =
      system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 2), feed.ref))

    // Send action that blocks the pool
    pool ! runMessageLarge
    containers(0).expectMsg(runMessageLarge)

    // Send action that should be written to the queue and retried in invoker
    pool ! runMessage
    containers(1).expectNoMessage(100.milliseconds)

    // Send another message that should not be retried, but put into the queue as well
    pool ! runMessageDifferentAction
    containers(2).expectNoMessage(100.milliseconds)

    // Action with 512 MB is finished
    containers(0).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)

    // Action 1 should start immediately
    containers(0).expectMsgPF() {
      // The `Some` assures, that it has been retried while the first action was still blocking the invoker.
      case Run(runMessage.action, runMessage.msg, Some(_)) => true
    }
    // Action 2 should start immediately as well (without any retries, as there is already enough space in the pool)
    containers(1).expectMsg(runMessageDifferentAction)
  }

  it should "process activations in the order they are arriving" in within(timeout) {
    val (containers, factory) = testContainers(4)
    val feed = TestProbe()

    // Pool with 512 MB usermemory
    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 2), feed.ref))

    // Send 4 actions to the ContainerPool (Action 0, Action 2 and Action 3 with each 265 MB and Action 1 with 512 MB)
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    pool ! runMessageLarge
    containers(1).expectNoMessage(100.milliseconds)
    pool ! runMessageDifferentNamespace
    containers(2).expectNoMessage(100.milliseconds)
    pool ! runMessageDifferentAction
    containers(3).expectNoMessage(100.milliseconds)

    // Action 0 ist finished -> Large action should be executed now
    containers(0).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
    containers(1).expectMsgPF() {
      // The `Some` assures, that it has been retried while the first action was still blocking the invoker.
      case Run(runMessageLarge.action, runMessageLarge.msg, Some(_)) => true
    }

    // Send another action to the container pool, that would fit memory-wise
    pool ! runMessageDifferentEverything
    containers(4).expectNoMessage(100.milliseconds)

    // Action 1 is finished -> Action 2 and Action 3 should be executed now
    containers(1).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
    containers(2).expectMsgPF() {
      // The `Some` assures, that it has been retried while the first action was still blocking the invoker.
      case Run(runMessageDifferentNamespace.action, runMessageDifferentNamespace.msg, Some(_)) => true
    }
    // Assert retryLogline = false to check if this request has been stored in the queue instead of retrying in the system
    containers(3).expectMsg(runMessageDifferentAction)

    // Action 3 is finished -> Action 4 should start
    containers(3).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
    containers(4).expectMsgPF() {
      // The `Some` assures, that it has been retried while the first action was still blocking the invoker.
      case Run(runMessageDifferentEverything.action, runMessageDifferentEverything.msg, Some(_)) => true
    }

    // Action 2 and 4 are finished
    containers(2).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
    containers(4).send(pool, NeedWork(warmedData()))
    feed.expectMsg(MessageFeed.Processed)
  }

  it should "increase activation counts when scheduling to containers whose actions support concurrency" in {
    assume(concurrencyEnabled)
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 4), feed.ref))

    // container0 is created and used
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)

    // container0 is reused
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)

    // container0 is reused
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)

    // container1 is created and used (these concurrent containers are configured with max 3 concurrent activations)
    pool ! runMessageConcurrent
    containers(1).expectMsg(runMessageConcurrent)
  }

  it should "schedule concurrent activations to different containers for different namespaces" in {
    assume(concurrencyEnabled)
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 4), feed.ref))

    // container0 is created and used
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)

    // container1 is created and used
    pool ! runMessageConcurrentDifferentNamespace
    containers(1).expectMsg(runMessageConcurrentDifferentNamespace)
  }

  it should "decrease activation counts when receiving NeedWork for actions that support concurrency" in {
    assume(concurrencyEnabled)
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()

    val pool = system.actorOf(ContainerPool.props(factory, poolConfig(MemoryLimit.stdMemory * 4), feed.ref))

    // container0 is created and used
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)

    // container0 is reused
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)

    // container0 is reused
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)

    // container1 is created and used (these concurrent containers are configured with max 3 concurrent activations)
    pool ! runMessageConcurrent
    containers(1).expectMsg(runMessageConcurrent)

    // container1 is reused
    pool ! runMessageConcurrent
    containers(1).expectMsg(runMessageConcurrent)

    // container1 is reused
    pool ! runMessageConcurrent
    containers(1).expectMsg(runMessageConcurrent)

    containers(0).send(pool, NeedWork(warmedData(action = concurrentAction)))

    // container0 is reused (since active count decreased)
    pool ! runMessageConcurrent
    containers(0).expectMsg(runMessageConcurrent)
  }
}

/**
 * Unit tests for the ContainerPool object.
 *
 * These tests test only the "static" methods "schedule" and "remove"
 * of the ContainerPool object.
 */
@RunWith(classOf[JUnitRunner])
class ContainerPoolObjectTests extends FlatSpec with Matchers with MockFactory {

  val actionExec = CodeExecAsString(RuntimeManifest("actionKind", ImageName("testImage")), "testCode", None)
  val standardNamespace = EntityName("standardNamespace")
  val differentNamespace = EntityName("differentNamespace")

  /** Helper to create a new action from String representations */
  def createAction(namespace: String = "actionNS", name: String = "actionName", limits: ActionLimits = ActionLimits()) =
    ExecutableWhiskAction(EntityPath(namespace), EntityName(name), actionExec, limits = limits)

  /** Helper to create WarmedData with sensible defaults */
  def warmedData(action: ExecutableWhiskAction = createAction(),
                 namespace: String = standardNamespace.asString,
                 lastUsed: Instant = Instant.now,
                 active: Int = 0) =
    WarmedData(stub[Container], EntityName(namespace), action, lastUsed, active)

  /** Helper to create WarmingData with sensible defaults */
  def warmingData(action: ExecutableWhiskAction = createAction(),
                  namespace: String = standardNamespace.asString,
                  lastUsed: Instant = Instant.now,
                  active: Int = 0) =
    WarmingData(stub[Container], EntityName(namespace), action, lastUsed, active)

  /** Helper to create WarmingData with sensible defaults */
  def warmingColdData(action: ExecutableWhiskAction = createAction(),
                      namespace: String = standardNamespace.asString,
                      lastUsed: Instant = Instant.now,
                      active: Int = 0) =
    WarmingColdData(EntityName(namespace), action, lastUsed, active)

  /** Helper to create PreWarmedData with sensible defaults */
  def preWarmedData(kind: String = "anyKind") = PreWarmedData(stub[Container], kind, 256.MB)

  /** Helper to create NoData */
  def noData() = NoData()

  behavior of "ContainerPool schedule()"

  it should "not provide a container if idle pool is empty" in {
    ContainerPool.schedule(createAction(), standardNamespace, Map.empty) shouldBe None
  }

  it should "reuse an applicable warm container from idle pool with one container" in {
    val data = warmedData()
    val pool = Map('name -> data)

    // copy to make sure, referencial equality doesn't suffice
    ContainerPool.schedule(data.action.copy(), data.invocationNamespace, pool) shouldBe Some('name, data)
  }

  it should "reuse an applicable warm container from idle pool with several applicable containers" in {
    val data = warmedData()
    val pool = Map('first -> data, 'second -> data)

    ContainerPool.schedule(data.action.copy(), data.invocationNamespace, pool) should (be(Some('first, data)) or be(
      Some('second, data)))
  }

  it should "reuse an applicable warm container from idle pool with several different containers" in {
    val matchingData = warmedData()
    val pool = Map('none -> noData(), 'pre -> preWarmedData(), 'warm -> matchingData)

    ContainerPool.schedule(matchingData.action.copy(), matchingData.invocationNamespace, pool) shouldBe Some(
      'warm,
      matchingData)
  }

  it should "not reuse a container from idle pool with non-warm containers" in {
    val data = warmedData()
    // data is **not** in the pool!
    val pool = Map('none -> noData(), 'pre -> preWarmedData())

    ContainerPool.schedule(data.action.copy(), data.invocationNamespace, pool) shouldBe None
  }

  it should "not reuse a warm container with different invocation namespace" in {
    val data = warmedData()
    val pool = Map('warm -> data)
    val differentNamespace = EntityName(data.invocationNamespace.asString + "butDifferent")

    data.invocationNamespace should not be differentNamespace
    ContainerPool.schedule(data.action.copy(), differentNamespace, pool) shouldBe None
  }

  it should "not reuse a warm container with different action name" in {
    val data = warmedData()
    val differentAction = data.action.copy(name = EntityName(data.action.name.asString + "butDifferent"))
    val pool = Map('warm -> data)

    data.action.name should not be differentAction.name
    ContainerPool.schedule(differentAction, data.invocationNamespace, pool) shouldBe None
  }

  it should "not reuse a warm container with different action version" in {
    val data = warmedData()
    val differentAction = data.action.copy(version = data.action.version.upMajor)
    val pool = Map('warm -> data)

    data.action.version should not be differentAction.version
    ContainerPool.schedule(differentAction, data.invocationNamespace, pool) shouldBe None
  }

  it should "not use a container when active activation count >= maxconcurrent" in {
    val concurrencyEnabled = Option(WhiskProperties.getProperty("whisk.action.concurrency")).exists(_.toBoolean)
    val maxConcurrent = if (concurrencyEnabled) 25 else 1

    val data = warmedData(
      active = maxConcurrent,
      action = createAction(limits = ActionLimits(concurrency = ConcurrencyLimit(maxConcurrent))))
    val pool = Map('warm -> data)
    ContainerPool.schedule(data.action, data.invocationNamespace, pool) shouldBe None

    val data2 = warmedData(
      active = maxConcurrent - 1,
      action = createAction(limits = ActionLimits(concurrency = ConcurrencyLimit(maxConcurrent))))
    val pool2 = Map('warm -> data2)

    ContainerPool.schedule(data2.action, data2.invocationNamespace, pool2) shouldBe Some('warm, data2)

  }

  it should "use a warming when active activation count < maxconcurrent" in {
    val concurrencyEnabled = Option(WhiskProperties.getProperty("whisk.action.concurrency")).exists(_.toBoolean)
    val maxConcurrent = if (concurrencyEnabled) 25 else 1

    val action = createAction(limits = ActionLimits(concurrency = ConcurrencyLimit(maxConcurrent)))
    val data = warmingData(active = maxConcurrent - 1, action = action)
    val pool = Map('warming -> data)
    ContainerPool.schedule(data.action, data.invocationNamespace, pool) shouldBe Some('warming, data)

    val data2 = warmedData(active = maxConcurrent - 1, action = action)
    val pool2 = pool ++ Map('warm -> data2)

    ContainerPool.schedule(data2.action, data2.invocationNamespace, pool2) shouldBe Some('warm, data2)
  }

  it should "prefer warm to warming when active activation count < maxconcurrent" in {
    val concurrencyEnabled = Option(WhiskProperties.getProperty("whisk.action.concurrency")).exists(_.toBoolean)
    val maxConcurrent = if (concurrencyEnabled) 25 else 1

    val action = createAction(limits = ActionLimits(concurrency = ConcurrencyLimit(maxConcurrent)))
    val data = warmingColdData(active = maxConcurrent - 1, action = action)
    val data2 = warmedData(active = maxConcurrent - 1, action = action)
    val pool = Map('warming -> data, 'warm -> data2)
    ContainerPool.schedule(data.action, data.invocationNamespace, pool) shouldBe Some('warm, data2)
  }

  it should "use a warmingCold when active activation count < maxconcurrent" in {
    val concurrencyEnabled = Option(WhiskProperties.getProperty("whisk.action.concurrency")).exists(_.toBoolean)
    val maxConcurrent = if (concurrencyEnabled) 25 else 1

    val action = createAction(limits = ActionLimits(concurrency = ConcurrencyLimit(maxConcurrent)))
    val data = warmingColdData(active = maxConcurrent - 1, action = action)
    val pool = Map('warmingCold -> data)
    ContainerPool.schedule(data.action, data.invocationNamespace, pool) shouldBe Some('warmingCold, data)

    //after scheduling, the pool will update with new data to set active = maxConcurrent
    val data2 = warmingColdData(active = maxConcurrent, action = action)
    val pool2 = Map('warmingCold -> data2)

    ContainerPool.schedule(data2.action, data2.invocationNamespace, pool2) shouldBe None
  }

  it should "prefer warm to warmingCold when active activation count < maxconcurrent" in {
    val concurrencyEnabled = Option(WhiskProperties.getProperty("whisk.action.concurrency")).exists(_.toBoolean)
    val maxConcurrent = if (concurrencyEnabled) 25 else 1

    val action = createAction(limits = ActionLimits(concurrency = ConcurrencyLimit(maxConcurrent)))
    val data = warmingColdData(active = maxConcurrent - 1, action = action)
    val data2 = warmedData(active = maxConcurrent - 1, action = action)
    val pool = Map('warmingCold -> data, 'warm -> data2)
    ContainerPool.schedule(data.action, data.invocationNamespace, pool) shouldBe Some('warm, data2)
  }

  it should "prefer warming to warmingCold when active activation count < maxconcurrent" in {
    val concurrencyEnabled = Option(WhiskProperties.getProperty("whisk.action.concurrency")).exists(_.toBoolean)
    val maxConcurrent = if (concurrencyEnabled) 25 else 1

    val action = createAction(limits = ActionLimits(concurrency = ConcurrencyLimit(maxConcurrent)))
    val data = warmingColdData(active = maxConcurrent - 1, action = action)
    val data2 = warmingData(active = maxConcurrent - 1, action = action)
    val pool = Map('warmingCold -> data, 'warming -> data2)
    ContainerPool.schedule(data.action, data.invocationNamespace, pool) shouldBe Some('warming, data2)
  }

  behavior of "ContainerPool remove()"

  it should "not provide a container if pool is empty" in {
    ContainerPool.remove(Map.empty, MemoryLimit.stdMemory) shouldBe List.empty
  }

  it should "not provide a container from busy pool with non-warm containers" in {
    val pool = Map('none -> noData(), 'pre -> preWarmedData())
    ContainerPool.remove(pool, MemoryLimit.stdMemory) shouldBe List.empty
  }

  it should "not provide a container from pool if there is not enough capacity" in {
    val pool = Map('first -> warmedData())

    ContainerPool.remove(pool, MemoryLimit.stdMemory * 2) shouldBe List.empty
  }

  it should "provide a container from pool with one single free container" in {
    val data = warmedData()
    val pool = Map('warm -> data)
    ContainerPool.remove(pool, MemoryLimit.stdMemory) shouldBe List('warm)
  }

  it should "provide oldest container from busy pool with multiple containers" in {
    val commonNamespace = differentNamespace.asString
    val first = warmedData(namespace = commonNamespace, lastUsed = Instant.ofEpochMilli(1))
    val second = warmedData(namespace = commonNamespace, lastUsed = Instant.ofEpochMilli(2))
    val oldest = warmedData(namespace = commonNamespace, lastUsed = Instant.ofEpochMilli(0))

    val pool = Map('first -> first, 'second -> second, 'oldest -> oldest)

    ContainerPool.remove(pool, MemoryLimit.stdMemory) shouldBe List('oldest)
  }

  it should "provide a list of the oldest containers from pool, if several containers have to be removed" in {
    val namespace = differentNamespace.asString
    val first = warmedData(namespace = namespace, lastUsed = Instant.ofEpochMilli(1))
    val second = warmedData(namespace = namespace, lastUsed = Instant.ofEpochMilli(2))
    val third = warmedData(namespace = namespace, lastUsed = Instant.ofEpochMilli(3))
    val oldest = warmedData(namespace = namespace, lastUsed = Instant.ofEpochMilli(0))

    val pool = Map('first -> first, 'second -> second, 'third -> third, 'oldest -> oldest)

    ContainerPool.remove(pool, MemoryLimit.stdMemory * 2) shouldBe List('oldest, 'first)
  }

  it should "provide oldest container (excluding concurrently busy) from busy pool with multiple containers" in {
    val commonNamespace = differentNamespace.asString
    val first = warmedData(namespace = commonNamespace, lastUsed = Instant.ofEpochMilli(1), active = 0)
    val second = warmedData(namespace = commonNamespace, lastUsed = Instant.ofEpochMilli(2), active = 0)
    val oldest = warmedData(namespace = commonNamespace, lastUsed = Instant.ofEpochMilli(0), active = 3)

    var pool = Map('first -> first, 'second -> second, 'oldest -> oldest)
    ContainerPool.remove(pool, MemoryLimit.stdMemory) shouldBe List('first)
    pool = pool - 'first
    ContainerPool.remove(pool, MemoryLimit.stdMemory) shouldBe List('second)
  }
}
