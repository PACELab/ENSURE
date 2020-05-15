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

package org.apache.openwhisk.core.database.cosmosdb

import com.microsoft.azure.cosmosdb.internal.Constants.Properties.{AGGREGATE, E_TAG, ID, SELF_LINK}
import org.apache.openwhisk.core.database.cosmosdb.CosmosDBConstants._
import spray.json.{JsObject, JsString, JsValue}

import scala.collection.immutable.Iterable

private[cosmosdb] object CosmosDBConstants {

  /**
   * Stores the computed properties required for view related queries
   */
  val computed: String = "_c"

  val alias: String = "view"

  val cid: String = ID

  val etag: String = E_TAG

  val aggregate: String = AGGREGATE

  val selfLink: String = SELF_LINK

  /**
   * Records the clusterId which performed changed in any document. This can vary over
   * lifetime of a document as different clusters may change the same document at different times
   */
  val clusterId: String = "_clusterId"

  /**
   * Property indicating that document has been marked as deleted with ttl
   */
  val deleted: String = "_deleted"
}

private[cosmosdb] trait CosmosDBUtil {

  /**
   * Name of `id` field as used in WhiskDocument
   */
  val _id: String = "_id"

  /**
   * Name of revision field as used in WhiskDocument
   */
  val _rev: String = "_rev"

  /**
   * Prepares the json like select clause
   * {{{
   *   Seq("a", "b", "c.d.e") =>
   *   { "a" : r['a'], "b" : r['b'], "c" : { "d" : { "e" : r['c']['d']['e']}}, "id" : r['id']} AS view
   * }}}
   * Here it uses {{{r['keyName']}}} notation to avoid issues around using reserved words as field name
   */
  def prepareFieldClause(fields: Iterable[String]): String = {
    val m = fields.foldLeft(Map.empty[String, Any]) { (map, name) =>
      addToMap(name, map)
    }
    val withId = addToMap(cid, m)
    val json = asJsonLikeString(withId)
    s"$json AS $alias"
  }

  private def addToMap(name: String, map: Map[String, _]): Map[String, Any] = name.split('.').toList match {
    case Nil     => throw new IllegalStateException(s"'$name' split on '.' should not result in empty list")
    case x :: xs => addToMap(x, xs, Nil, map)
  }

  private def addToMap(key: String,
                       children: List[String],
                       keyPath: List[String],
                       map: Map[String, Any]): Map[String, Any] = children match {
    case Nil => map + (key -> s"r${makeKeyPath(key :: keyPath)}")
    case x :: xs =>
      map + (key -> addToMap(x, xs, key :: keyPath, map.getOrElse(key, Map.empty).asInstanceOf[Map[String, Any]]))
  }

  private def makeKeyPath(keyPath: List[String]) = keyPath.reverse.map(f => s"['$f']").mkString

  private def asJsonLikeString(m: Map[_, _]) =
    m.map { case (k, v) => s""" "$k" : ${asString(v)}""" }.mkString("{", ",", "}")

  private def asString(v: Any): String = v match {
    case m: Map[_, _] => asJsonLikeString(m)
    case x            => x.toString
  }

  /**
   * CosmosDB id considers '/', '\' , '?' and '#' as invalid. EntityNames can include '/' so
   * that need to be escaped. For that we use '|' as the replacement char
   */
  def escapeId(id: String): String = {
    require(!id.contains("|"), s"Id [$id] should not contain '|'")
    id.replace("/", "|")
  }

  def unescapeId(id: String): String = {
    require(!id.contains("/"), s"Escaped Id [$id] should not contain '/'")
    id.replace("|", "/")
  }

  def toWhiskJsonDoc(js: JsObject, id: String, etag: Option[JsString]): JsObject = {
    val fieldsToAdd = Seq((_id, Some(JsString(unescapeId(id)))), (_rev, etag))
    transform(stripInternalFields(js), fieldsToAdd, Seq.empty)
  }

  /**
   * Transforms a json object by adding and removing fields
   *
   * @param json base json object to transform
   * @param fieldsToAdd list of fields to add. If the value provided is `None` then it would be ignored
   * @param fieldsToRemove list of field names to remove
   * @return transformed json
   */
  def transform(json: JsObject,
                fieldsToAdd: Seq[(String, Option[JsValue])],
                fieldsToRemove: Seq[String] = Seq.empty): JsObject = {
    val fields = json.fields ++ fieldsToAdd.flatMap(f => f._2.map((f._1, _))) -- fieldsToRemove
    JsObject(fields)
  }

  private def stripInternalFields(js: JsObject) = {
    //Strip out all field name starting with '_' which are considered as db specific internal fields
    JsObject(js.fields.filter { case (k, _) => !k.startsWith("_") && k != cid })
  }

}

private[cosmosdb] object CosmosDBUtil extends CosmosDBUtil
