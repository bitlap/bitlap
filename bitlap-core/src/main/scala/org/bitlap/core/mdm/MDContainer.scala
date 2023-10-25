/**
 * Copyright (C) 2023 bitlap.org .
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
