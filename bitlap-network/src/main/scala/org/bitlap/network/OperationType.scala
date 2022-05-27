/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.driver.proto.BOperationType

/** @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
object OperationType extends Enumeration {

  type OperationType = Value

  val EXECUTE_STATEMENT: OperationType.Value = Value(
    BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT.index,
    BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT.name
  )
  val GET_COLUMNS: OperationType.Value =
    Value(
      BOperationType.B_OPERATION_TYPE_GET_COLUMNS.index,
      BOperationType.B_OPERATION_TYPE_GET_COLUMNS.name
    )
  val GET_SCHEMAS: OperationType.Value =
    Value(
      BOperationType.B_OPERATION_TYPE_GET_SCHEMAS.index,
      BOperationType.B_OPERATION_TYPE_GET_SCHEMAS.name
    )
  val GET_TABLES: OperationType.Value =
    Value(
      BOperationType.B_OPERATION_TYPE_GET_TABLES.index,
      BOperationType.B_OPERATION_TYPE_GET_TABLES.name
    )
  val UNKNOWN_OPERATION: OperationType.Value =
    Value(0, "Unknown")

  def getOperationType(bOperationType: BOperationType): OperationType = {
    val idx = bOperationType match {
      case BOperationType.B_OPERATION_TYPE_GET_TABLES        => BOperationType.B_OPERATION_TYPE_GET_TABLES.index
      case BOperationType.B_OPERATION_TYPE_UNSPECIFIED       => BOperationType.B_OPERATION_TYPE_UNSPECIFIED.index
      case BOperationType.B_OPERATION_TYPE_GET_COLUMNS       => BOperationType.B_OPERATION_TYPE_GET_COLUMNS.index
      case BOperationType.B_OPERATION_TYPE_GET_SCHEMAS       => BOperationType.B_OPERATION_TYPE_GET_SCHEMAS.index
      case BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT => BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT.index
      case BOperationType.Unrecognized(i)                    => i
    }
    values.find(_.id == idx).getOrElse(UNKNOWN_OPERATION)
  }

  def toBOperationType(operationType: OperationType): BOperationType = BOperationType.fromValue(operationType.id)

}
