/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network

import org.bitlap.network.Driver.*
import org.bitlap.network.enumeration.*

import com.google.protobuf.ByteString

/** bitlap 数据模型，和数据传输模型（proto）的变换
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
object models:

  /** ADTs，所有类必须继承自此接口，继承类还必须提供toXX和fromXX方法
   */
  sealed trait Model

  final case class QueryResultSet(tableSchema: TableSchema, rows: RowSet) extends Model

  final case class RowSet(rows: List[Row] = Nil, startOffset: Long = 0) extends Model:
    def toBRowSet: BRowSet = BRowSet(startRowOffset = startOffset, rows = rows.map(_.toBRow))

  object RowSet:

    def fromBRowSet(bRowSet: BRowSet): RowSet =
      RowSet(bRowSet.rows.map(f => Row(f.colVals.toList)).toList, bRowSet.startRowOffset)

  final case class FetchResults(hasMoreRows: Boolean, results: RowSet) extends Model:

    def toBFetchResultsResp: BFetchResultsResp =
      BFetchResultsResp(hasMoreRows, Some(results.toBRowSet))

  object FetchResults:

    def fromBFetchResultsResp(bFetchResults: BFetchResultsResp): FetchResults =
      FetchResults(
        bFetchResults.hasMoreRows,
        RowSet.fromBRowSet(bFetchResults.getResults)
      )

  /** The wrapper class of the Proto buffer [[org.bitlap.network.Driver.BRow]].
   */
  final case class Row(values: List[ByteString] = Nil) extends Model:
    def toBRow: BRow = BRow(values)

  final case class TableSchema(columns: List[ColumnDesc] = Nil) extends Model:

    def toBGetResultSetMetadataResp: BGetResultSetMetadataResp =
      BGetResultSetMetadataResp(Option(BTableSchema(columns = columns.map(_.toBColumnDesc))))

  object TableSchema:

    def fromBGetResultSetMetadataResp(getResultSetMetadataResp: BGetResultSetMetadataResp): TableSchema =
      TableSchema(
        getResultSetMetadataResp.getSchema.columns.map(b => ColumnDesc.fromBColumnDesc(b)).toList
      )

  /** The wrapper class of the Proto buffer [[org.bitlap.network.Driver.BColumnDesc]]
   */
  final case class ColumnDesc(columnName: String, typeDesc: TypeId) extends Model:

    def toBColumnDesc: BColumnDesc =
      BColumnDesc(typeDesc = TypeId.toBTypeId(typeDesc), columnName = columnName)

  object ColumnDesc:

    def fromBColumnDesc(bColumnDesc: BColumnDesc): ColumnDesc =
      ColumnDesc(bColumnDesc.columnName, TypeId.toTypeId(bColumnDesc.typeDesc))

  final case class OperationStatus(hasResultSet: Option[Boolean], status: Option[OperationState]) extends Model:

    def toBGetOperationStatusResp: BGetOperationStatusResp =
      BGetOperationStatusResp(status.map(OperationState.toBOperationState), hasResultSet)

  object OperationStatus:

    def fromBGetOperationStatusResp(getOperationStatusResp: BGetOperationStatusResp): OperationStatus =
      OperationStatus(
        getOperationStatusResp.hasResultSet,
        getOperationStatusResp.operationState.map(OperationState.toOperationState)
      )

  final case class GetInfoValue(value: ByteString) extends Model:
    def toBGetInfoResp: BGetInfoResp = BGetInfoResp(Option(BGetInfoValue(value)))

  object GetInfoValue:
    def fromBGetInfoResp(getInfoResp: BGetInfoResp): GetInfoValue = GetInfoValue(getInfoResp.getInfoValue.value)
