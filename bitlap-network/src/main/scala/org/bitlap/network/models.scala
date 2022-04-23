/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.google.protobuf.ByteString
import org.bitlap.network.driver.proto.BFetchResults.BFetchResultsResp
import org.bitlap.network.driver.proto._
import org.bitlap.network.models.StatusCode.StatusCode
import org.bitlap.network.models.TypeId.TypeId

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
object models {

  // client and server should use those models instead of using protobuf models
  sealed trait Model

  final case class QueryResult(tableSchema: TableSchema, rows: RowSet) extends Model

  final case class RowSet(rows: List[Row] = Nil, startOffset: Long = 0) extends Model {
    def toBRowSet: BRowSet = BRowSet(startRowOffset = startOffset, rows = rows.map(_.toBRow))
  }
  object RowSet {
    def fromBRowSet(bRowSet: BRowSet): RowSet =
      RowSet(bRowSet.rows.map(f => Row(f.colVals.toList)).toList, bRowSet.startRowOffset)
  }

  final case class FetchResults(
    hasMoreRows: Boolean,
    results: RowSet,
    status: Option[Status] = Some(models.Status(StatusCode.STATUS_CODE_SUCCESS_STATUS))
  ) extends Model {
    def toBFetchResults: BFetchResultsResp =
      BFetchResultsResp(status.map(_.toBStatus), hasMoreRows, Some(results.toBRowSet))
  }

  object FetchResults {
    def fromBFetchResultsResp(bFetchResults: BFetchResultsResp): FetchResults =
      FetchResults(
        bFetchResults.hasMoreRows,
        RowSet.fromBRowSet(bFetchResults.getResults),
        Some(Status.fromBStatus(bFetchResults.getStatus))
      )
  }

  final case class Status(statusCode: StatusCode, sqlState: String = "", errorCode: Int = 0, errorMessage: String = "")
      extends Model {
    def toBStatus: BStatus = BStatus(
      StatusCode.toStatusCode(statusCode),
      sqlState,
      errorCode,
      errorMessage
    )
  }
  object Status {
    def fromBStatus(bStatus: BStatus): Status = Status(
      StatusCode.fromBStatusCode(bStatus.statusCode),
      bStatus.sqlState,
      bStatus.errorCode,
      bStatus.errorMessage
    )
  }

  /**
   * The wrapper class of the Proto buffer `BRow`.
   */
  final case class Row(values: List[ByteString] = Nil) extends Model {
    def toBRow: BRow = BRow(values)
  }

  final case class TableSchema(columns: List[ColumnDesc] = Nil) extends Model {

    def toBTableSchema: BTableSchema = BTableSchema(columns = columns.map(_.toBColumnDesc))
  }

  object TableSchema {
    def fromBTableSchema(bTableSchema: BTableSchema): TableSchema =
      TableSchema(
        bTableSchema.columns.map(b => ColumnDesc.fromBColumnDesc(b)).toList
      )
  }

  /**
   * The wrapper class of the Proto buffer `BColumnDesc`.
   */
  final case class ColumnDesc(
    columnName: String,
    typeDesc: TypeId
  ) extends Model {

    def toBColumnDesc: BColumnDesc =
      BColumnDesc(typeDesc = TypeId.toBOperationType(typeDesc), columnName = columnName)
  }

  object ColumnDesc {
    def fromBColumnDesc(bColumnDesc: BColumnDesc): ColumnDesc =
      ColumnDesc(bColumnDesc.columnName, TypeId.fromBOperationType(bColumnDesc.typeDesc))
  }

  /**
   * The wrapper class of the Proto buffer `BTypeId`.
   */
  object TypeId extends Enumeration {

    // define types <=> java.sql.Types in driver
    type TypeId = Value
    val TYPE_ID_UNSPECIFIED: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_UNSPECIFIED.index,
        BTypeId.B_TYPE_ID_UNSPECIFIED.name
      )
    val TYPE_ID_STRING_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_STRING_TYPE.index,
        BTypeId.B_TYPE_ID_STRING_TYPE.name
      )
    val TYPE_ID_INT_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_INT_TYPE.index,
        BTypeId.B_TYPE_ID_INT_TYPE.name
      )
    val TYPE_ID_DOUBLE_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_DOUBLE_TYPE.index,
        BTypeId.B_TYPE_ID_DOUBLE_TYPE.name
      )
    val TYPE_ID_LONG_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_LONG_TYPE.index,
        BTypeId.B_TYPE_ID_LONG_TYPE.name
      )
    val TYPE_ID_BOOLEAN_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_BOOLEAN_TYPE.index,
        BTypeId.B_TYPE_ID_BOOLEAN_TYPE.name
      )
    val TYPE_ID_TIMESTAMP_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_TIMESTAMP_TYPE.index,
        BTypeId.B_TYPE_ID_TIMESTAMP_TYPE.name
      )
    val TYPE_ID_SHORT_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_SHORT_TYPE.index,
        BTypeId.B_TYPE_ID_SHORT_TYPE.name
      )

    val TYPE_ID_BYTE_TYPE: TypeId.Value =
      Value(
        BTypeId.B_TYPE_ID_BYTE_TYPE.index,
        BTypeId.B_TYPE_ID_BYTE_TYPE.name
      )

    def toBOperationType(typeId: TypeId): BTypeId =
      BTypeId.fromValue(typeId.id)

    def fromBOperationType(bTypeId: BTypeId): TypeId = bTypeId match {
      case BTypeId.B_TYPE_ID_INT_TYPE       => TypeId.TYPE_ID_INT_TYPE
      case BTypeId.B_TYPE_ID_LONG_TYPE      => TypeId.TYPE_ID_LONG_TYPE
      case BTypeId.B_TYPE_ID_BYTE_TYPE      => TypeId.TYPE_ID_BYTE_TYPE
      case BTypeId.B_TYPE_ID_SHORT_TYPE     => TypeId.TYPE_ID_SHORT_TYPE
      case BTypeId.B_TYPE_ID_DOUBLE_TYPE    => TypeId.TYPE_ID_DOUBLE_TYPE
      case BTypeId.B_TYPE_ID_UNSPECIFIED    => TypeId.TYPE_ID_UNSPECIFIED
      case BTypeId.B_TYPE_ID_BOOLEAN_TYPE   => TypeId.TYPE_ID_BOOLEAN_TYPE
      case BTypeId.B_TYPE_ID_STRING_TYPE    => TypeId.TYPE_ID_STRING_TYPE
      case BTypeId.B_TYPE_ID_TIMESTAMP_TYPE => TypeId.TYPE_ID_TIMESTAMP_TYPE
    }
  }

  /**
   * The wrapper class of the Proto buffer `BStatusCode`.
   */
  object StatusCode extends Enumeration {

    type StatusCode = Value

    val STATUS_CODE_ERROR_STATUS: models.StatusCode.Value =
      Value(BStatusCode.B_STATUS_CODE_ERROR_STATUS.index, BStatusCode.B_STATUS_CODE_ERROR_STATUS.name)
    val STATUS_CODE_SUCCESS_STATUS: models.StatusCode.Value =
      Value(BStatusCode.B_STATUS_CODE_SUCCESS_STATUS.index, BStatusCode.B_STATUS_CODE_SUCCESS_STATUS.name)
    val STATUS_CODE_STILL_EXECUTING_STATUS: models.StatusCode.Value = Value(
      BStatusCode.B_STATUS_CODE_STILL_EXECUTING_STATUS.index,
      BStatusCode.B_STATUS_CODE_STILL_EXECUTING_STATUS.name
    )
    val STATUS_CODE_INVALID_HANDLE_STATUS: models.StatusCode.Value =
      Value(BStatusCode.B_STATUS_CODE_INVALID_HANDLE_STATUS.index, BStatusCode.B_STATUS_CODE_INVALID_HANDLE_STATUS.name)

    def toStatusCode(statusCode: StatusCode): BStatusCode =
      BStatusCode.fromValue(statusCode.id)

    def fromBStatusCode(bStatusCode: BStatusCode): StatusCode = bStatusCode match {
      case BStatusCode.B_STATUS_CODE_INVALID_HANDLE_STATUS  => StatusCode.STATUS_CODE_INVALID_HANDLE_STATUS
      case BStatusCode.B_STATUS_CODE_STILL_EXECUTING_STATUS => StatusCode.STATUS_CODE_STILL_EXECUTING_STATUS
      case BStatusCode.B_STATUS_CODE_SUCCESS_STATUS         => StatusCode.STATUS_CODE_SUCCESS_STATUS
      case BStatusCode.B_STATUS_CODE_ERROR_STATUS           => StatusCode.STATUS_CODE_ERROR_STATUS
    }

  }

}
