/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.google.protobuf.ByteString
import org.bitlap.network.models.TypeId.TypeId
import org.bitlap.network.proto.driver.{ BColumnDesc, BRow, BRowSet, BTableSchema, BTypeId }
import scala.jdk.CollectionConverters._

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
object models {

  case class QueryResult(tableSchema: TableSchema, rows: RowSet)

  case class RowSet(rows: List[Row] = Nil, startOffset: Long = 0) {
    def toBRowSet(): BRowSet =
      BRowSet
        .newBuilder()
        .setStartRowOffset(startOffset)
        .addAllRows(rows.map(_.toBRow()).asJava)
        .build()
  }

  /**
   * The wrapper class of the Proto buffer `BRow`.
   */
  case class Row(private val values: List[ByteString] = Nil) {
    def toBRow(): BRow = {
      import scala.jdk.CollectionConverters._
      BRow.newBuilder().addAllColVals(values.asJava).build()
    }
  }

  case class TableSchema(private val columns: List[ColumnDesc] = Nil) {

    def toBTableSchema(): BTableSchema =
      BTableSchema
        .newBuilder()
        .addAllColumns(columns.map(_.toBColumnDesc()).asJava)
        .build()
  }

  /**
   * The wrapper class of the Proto buffer `BColumnDesc`.
   */
  case class ColumnDesc(
    private val columnName: String,
    private val typeDesc: TypeId
  ) {

    def toBColumnDesc(): BColumnDesc =
      BColumnDesc
        .newBuilder()
        .setTypeDesc(TypeId.toBOperationType(typeDesc))
        .setColumnName(columnName)
        .build()
  }

  /**
   * The wrapper class of the Proto buffer `BTypeId`.
   */
  object TypeId extends Enumeration {

    type TypeId = Value
    val B_TYPE_ID_UNSPECIFIED: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_UNSPECIFIED.getNumber,
        BTypeId.B_TYPE_ID_UNSPECIFIED.name()
      )
    val B_TYPE_ID_STRING_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_STRING_TYPE.getNumber,
        BTypeId.B_TYPE_ID_STRING_TYPE.name()
      )
    val B_TYPE_ID_INT_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_INT_TYPE.getNumber,
        BTypeId.B_TYPE_ID_INT_TYPE.name()
      )
    val B_TYPE_ID_DOUBLE_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_DOUBLE_TYPE.getNumber,
        BTypeId.B_TYPE_ID_DOUBLE_TYPE.name()
      )
    val B_TYPE_ID_LONG_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_LONG_TYPE.getNumber,
        BTypeId.B_TYPE_ID_LONG_TYPE.name()
      )
    val B_TYPE_ID_BOOLEAN_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_BOOLEAN_TYPE.getNumber,
        BTypeId.B_TYPE_ID_BOOLEAN_TYPE.name()
      )
    val B_TYPE_ID_TIMESTAMP_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_TIMESTAMP_TYPE.getNumber,
        BTypeId.B_TYPE_ID_TIMESTAMP_TYPE.name()
      )
    val B_TYPE_ID_SHORT_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_SHORT_TYPE.getNumber,
        BTypeId.B_TYPE_ID_SHORT_TYPE.name()
      )

    def toBOperationType(typeId: TypeId): BTypeId =
      BTypeId.forNumber(typeId.id)
  }

}
