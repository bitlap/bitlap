/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.writer

import org.apache.spark.sql.types.StructType

import java.util.Properties

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/16
 */
final class BitlapDataSourceWriteOptions(
  val tableName: String,
  val url: String,
  val scan: String,
  val tenantId: String,
  val schema: StructType,
  val overriddenProps: Properties,
  val skipNormalizingIdentifier: Boolean)
    extends Serializable
