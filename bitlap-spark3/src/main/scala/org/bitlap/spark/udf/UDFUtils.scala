/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.spark.udf

import scala.collection.mutable

import org.bitlap.roaringbitmap.x.{ BBM, BMUtils, CBM, RBM }

import org.apache.commons.lang3.{ ClassUtils, StringUtils }

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** bitlap internal udfs
 */
object UDFUtils {

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(obj: Any): String =
    obj match {
      case o: String                                        => o
      case o if ClassUtils.isPrimitiveOrWrapper(o.getClass) => o.toString
      case o                                                => mapper.writeValueAsString(o)
    }

  def fromJson(json: String): Map[String, String] = {
    if (StringUtils.isBlank(json)) {
      return Map.empty
    }
    mapper.readValue(json, new TypeReference[Map[String, String]] {})
  }

  def map2String(map: Map[String, String]): String = {
    if (map == null || map.isEmpty) " "
    else {
      val treeMap = new mutable.TreeMap[String, String]()
      map.foreach { case (k, v) =>
        treeMap += k -> v
      }
      treeMap.toString
    }
  }

  def formatMap(map: Map[String, String]): Map[String, String] = {
    Option(map).map { m =>
      m.map { case (k, v) =>
        if (StringUtils.isBlank(v)) k -> " "
        else (k, v.substring(0, math.min(v.length, 1024)))
      }
    }.getOrElse(Map.empty)
  }

  // ------------------------------------------------------------
  // bitmap functions
  // ------------------------------------------------------------
  def rbmCount(bytes: Array[Byte]): Long = BMUtils.fromBytes(bytes, new RBM()).getCountUnique

  def bbmCount(bytes: Array[Byte]): Long = BMUtils.fromBytes(bytes, new BBM()).getCount.toLong

  def cbmCount(bytes: Array[Byte]): Double = BMUtils.fromBytes(bytes, new CBM()).getCount
}
