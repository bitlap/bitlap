/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.reader
import java.util.Properties

/** @since 2022/10/16
 *  @author
 *    梦境迷离
 */
final class BitlapDataSourceReadOptions(
  val url: String,
  val scan: String,
  val tenantId: String,
  val overriddenProps: Properties,
  val selectStatement: String
) extends Serializable {}
