package org.bitlap.network.types

import org.bitlap.network.proto.driver.BOperationType

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
object OperationType extends Enumeration {

  type OperationType = Value

  val EXECUTE_STATEMENT: OperationType.Value = Value(
    BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT.getNumber,
    BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT.name()
  )
  val GET_COLUMNS: OperationType.Value =
    Value(
      BOperationType.B_OPERATION_TYPE_GET_COLUMNS.getNumber,
      BOperationType.B_OPERATION_TYPE_GET_COLUMNS.name()
    )
  val GET_SCHEMAS: OperationType.Value =
    Value(
      BOperationType.B_OPERATION_TYPE_GET_SCHEMAS.getNumber,
      BOperationType.B_OPERATION_TYPE_GET_SCHEMAS.name()
    )
  val GET_TABLES: OperationType.Value =
    Value(
      BOperationType.B_OPERATION_TYPE_GET_TABLES.getNumber,
      BOperationType.B_OPERATION_TYPE_GET_TABLES.name()
    )
  val UNKNOWN_OPERATION: OperationType.Value =
    Value(0, BOperationType.UNRECOGNIZED.name())

  def getOperationType(bOperationType: BOperationType): OperationType =
    values.find(_.id == bOperationType.getNumber).getOrElse(UNKNOWN_OPERATION)

  def toBOperationType(operationType: OperationType): BOperationType =
    BOperationType.forNumber(operationType.id)

}
