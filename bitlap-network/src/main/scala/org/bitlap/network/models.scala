/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.google.protobuf.ByteString
import enumeratum.values._
import org.bitlap.network.driver.proto._
import org.bitlap.network.driver.proto.BFetchResults.BFetchResultsResp
import org.bitlap.network.driver.proto.BGetOperationStatus.BGetOperationStatusResp
import org.bitlap.network.OperationState.toBOperationState
import java.sql.Types

/** bitlap 数据模型，和数据传输模型（proto）的变换
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
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
    results: RowSet
  ) extends Model {
    def toBFetchResultsResp: BFetchResultsResp =
      BFetchResultsResp(hasMoreRows, Some(results.toBRowSet))
  }

  object FetchResults {
    def fromBFetchResultsResp(bFetchResults: BFetchResultsResp): FetchResults =
      FetchResults(
        bFetchResults.hasMoreRows,
        RowSet.fromBRowSet(bFetchResults.getResults)
      )
  }

  /** The wrapper class of the Proto buffer `BRow`.
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

  /** The wrapper class of the Proto buffer `BColumnDesc`.
   */
  final case class ColumnDesc(
    columnName: String,
    typeDesc: TypeId
  ) extends Model {

    def toBColumnDesc: BColumnDesc =
      BColumnDesc(typeDesc = TypeId.toBTypeId(typeDesc), columnName = columnName)
  }

  object ColumnDesc {
    def fromBColumnDesc(bColumnDesc: BColumnDesc): ColumnDesc =
      ColumnDesc(bColumnDesc.columnName, TypeId.toTypeId(bColumnDesc.typeDesc))
  }

  sealed abstract class TypeId(val value: Int, val name: String) extends IntEnumEntry

  object TypeId extends IntEnum[TypeId] {
    final case object Unspecified   extends TypeId(0, "Any")
    final case object StringType    extends TypeId(1, "String")
    final case object IntType       extends TypeId(2, "Int")
    final case object DoubleType    extends TypeId(3, "Double")
    final case object LongType      extends TypeId(4, "Long")
    final case object BooleanType   extends TypeId(5, "Boolean")
    final case object TimestampType extends TypeId(6, "Timestamp")
    final case object ShortType     extends TypeId(7, "Short")
    final case object ByteType      extends TypeId(8, "Byte")
    final case object FloatType     extends TypeId(9, "Float")
    final case object TimeType      extends TypeId(10, "Time")
    final case object DateType      extends TypeId(11, "Date")

    val values: IndexedSeq[TypeId] = findValues
    def toTypeId(bTypeId: BTypeId): TypeId =
      TypeId.withValueOpt(bTypeId.value).getOrElse(Unspecified)
    def toBTypeId(typeId: TypeId): BTypeId =
      BTypeId.fromValue(typeId.value)

    def jdbc2Bitlap: Map[Int, TypeId] = Map(
      Types.VARCHAR   -> TypeId.StringType,
      Types.SMALLINT  -> TypeId.ShortType,
      Types.INTEGER   -> TypeId.IntType,
      Types.BIGINT    -> TypeId.LongType,
      Types.DOUBLE    -> TypeId.DoubleType,
      Types.BOOLEAN   -> TypeId.BooleanType,
      Types.TIMESTAMP -> TypeId.TimestampType,
      Types.TINYINT   -> TypeId.ByteType,
      Types.FLOAT     -> TypeId.FloatType,
      Types.TIME      -> TypeId.TimeType,
      Types.DATE      -> TypeId.DateType
    )

    def bitlap2Jdbc: Map[TypeId, Int] = jdbc2Bitlap.map(kv => kv._2 -> kv._1)

  }

  final case class OperationStatus(
    hasResultSet: Option[Boolean],
    status: Option[OperationState]
  ) extends Model {
    def toBGetOperationStatusResp: BGetOperationStatusResp =
      BGetOperationStatusResp(status.map(toBOperationState), hasResultSet)
  }

  object OperationStatus {
    def fromBOperationStatusResp(getOperationStatusResp: BGetOperationStatusResp): OperationStatus =
      OperationStatus(
        getOperationStatusResp.hasResultSet,
        getOperationStatusResp.operationState.map(OperationState.toOperationState)
      )
  }

}
