/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
trait SparkData extends Product with Serializable

object SparkData {
  final case class Dimension(key: String, value: String) extends SparkData
}
