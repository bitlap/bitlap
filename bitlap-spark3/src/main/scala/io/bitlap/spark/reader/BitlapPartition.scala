/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.reader

import org.apache.spark.sql.connector.read.InputPartition

/** @since 2022/10/16
 *  @author
 *    梦境迷离
 */
final class BitlapPartition(
  bitlapInputSplit: BitlapPartitionSplit
) extends InputPartition {}

// TODO
final class BitlapPartitionSplit extends Serializable
