/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
final case class Metric(
  time: Long,
  entity: Int,
  dimensions: List[Dimension],
  name: String,
  value: Long)
