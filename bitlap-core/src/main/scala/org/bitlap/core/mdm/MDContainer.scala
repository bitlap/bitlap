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
package org.bitlap.core.mdm

import java.util

import scala.jdk.CollectionConverters.*

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.storage.load.HasMetricKey

/** metric or dimension container to manager kv pairs
 */
class MDContainer[I <: HasMetricKey, R](private val keySize: Int)
    extends util.LinkedHashMap[List[Any], util.HashMap[String, R]] {

  def put(
    key: Any,
    row: I,
    initial: I => R,
    reducer: (R, I) => R
  ): MDContainer[I, R] = {
    this.put(List(key), row, initial, reducer)
  }

  def put(
    key: List[Any],
    row: I,
    initial: I => R,
    reducer: (R, I) => R
  ): MDContainer[I, R] = {
    PreConditions.checkExpression(key.size == keySize)
    val value = computeIfAbsent(
      key,
      (t: List[Any]) => util.HashMap[String, R]()
    )
    value.put(
      row.metricKey,
      if (value.containsKey(row.metricKey)) {
        reducer(value.get(row.metricKey), row)
      } else {
        initial(row)
      }
    )
    this
  }

  def flatRows(metrics: List[String])(defaultValue: () => Any): List[Row] = {

    this.asScala.map { case (keys, value) =>
      val arr = Array.fill[Any](keySize + metrics.size)(null)
      keys.zipWithIndex.foreach { case (key, i) => arr(i) = key }
      metrics.zipWithIndex.foreach { case (p, i) =>
        arr(i + keySize) = value.getOrDefault(p, defaultValue().asInstanceOf[R])
      }
      new Row(arr)
    }.toList
  }
}
