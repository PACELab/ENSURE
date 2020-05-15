// avs --begin
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

package org.apache.openwhisk.core.entity

import com.typesafe.config.ConfigFactory
import org.apache.openwhisk.core.ConfigKeys
import pureconfig._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._

case class InferredLimitConfig(
  min: Int, 
  max: Int, 
  std: Int)
  //mincpu: Int,
  //mostcpu: Int)

class InferredConfig(
  var mostusedCpuShares: Int = 32
  ) {
  var numTimesUpdated: Int = 0
}

/**
 * InferredLimit encapsulates allowed iVals in a single container for an action. The limit must be within a
 * permissible range (by default [1, 500]).
 *
 * It is a value type (hence == is .equals, immutable and cannot be assigned null).
 * The constructor is private so that argument requirements are checked and normalized
 * before creating a new instance.
 *
 * @param maxInferredVal the max number of concurrent activations in a single container
 */
//protected[entity] class InferredLimit private (val maxInferredVal: Int) extends AnyVal // avs --commented
//protected[entity] class InferredLimit private (val myInferredConfig: InferredConfig) extends AnyVal //avs
protected[entity] class InferredLimit private (val myInferredConfig: InferredConfig) extends AnyVal //avs

protected[core] object InferredLimit extends ArgNormalizer[InferredLimit] {
  //since tests require override to the default config, load the "test" config, with fallbacks to default
  val config = ConfigFactory.load().getConfig("test")
  private val inferredValConfig = loadConfigWithFallbackOrThrow[InferredLimitConfig](config, ConfigKeys.inferredLimit)

  protected[core] val minInferredVal: Int = inferredValConfig.min
  protected[core] val maxInferredVal: Int = inferredValConfig.max
  protected[core] val stdInferredVal: Int = inferredValConfig.std
  //protected[core] val mincpuInferredVal: Int = inferredValConfig.mincpu
  //protected[core] val mostcpuInferredVal: Int = inferredValConfig.mostcpu

  // avs - Adding values, instead of overriding everything that came by default. Can remove them, once i have everything I wanted.
  // Actual inferred parameters live here!
  //var myInferredConfig: InferredConfig = new InferredConfig(stdInferredVal)
  // inferred parameters end their existence here!!!

  /** Gets InferredLimit with default value */
  //protected[core] def apply(): InferredLimit = InferredLimit(stdInferredVal) // avs --commented
  protected[core] def apply(): InferredLimit = {
    val tempInferredConfig: InferredConfig = new InferredConfig(stdInferredVal)
    InferredLimit(tempInferredConfig) //avs 
  }

  /**
   * Creates InferredLimit for limit, iff limit is within permissible range.
   *
   * @param iVals the limit, must be within permissible range
   * @return InferredLimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */

  
  @throws[IllegalArgumentException]
  //protected[core] def apply(iVals: Int): InferredLimit = { //new InferredLimit(iVals) // avs --commented
  protected[core] def apply(iVals: InferredConfig): InferredLimit = {
    require(iVals.mostusedCpuShares >= minInferredVal, s"inferred $iVals below allowed threshold of $minInferredVal")
    require(iVals.mostusedCpuShares <= maxInferredVal, s"inferred $iVals exceeds allowed threshold of $maxInferredVal")

    //new InferredLimit(iVals) // avs --commented
    new InferredLimit(iVals) // avs
  }

  override protected[core] implicit val serdes = new RootJsonFormat[InferredLimit] {
    //def write(m: InferredLimit) = JsNumber(m.maxInferredVal) // avs --commented
    def write(m: InferredLimit) = JsNumber(m.myInferredConfig.mostusedCpuShares) 

    def read(value: JsValue) = {
      Try {
        val JsNumber(c) = value
        //require(c.isWhole(), "iVals limit must be whole number") // avs --commented

        //InferredLimit(c.toInt) // avs --commented
        //new InferredLimit(tempInferredConfig) // avs --commented
        val tempInferredConfig: InferredConfig = new InferredConfig(c.toInt) // avs
        new InferredLimit(tempInferredConfig) // avs
      } match {
        case Success(limit)                       => limit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable)                => deserializationError("inferred limit malformed", e)
      }
    }
  }
}
// avs --end
