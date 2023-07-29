/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.spark.udf

import org.bitlap.common.bitmap.CBM

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions.{ Expression, ExpressionDescription }
import org.apache.spark.sql.catalyst.expressions.aggregate.{ ImperativeAggregate, TypedImperativeAggregate }
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.types.{ BinaryType, DataType, IntegerType, LongType, NullType }

/** Collect bucket, id and count into a [[CBM]].
 */
@ExpressionDescription(
  usage = """
      _FUNC_(bucket, id, count) - Collect `bucket`, `id` and `count` into a CBM.
    """
)
final case class CBMAggr(
  bucket: Expression,
  id: Expression,
  count: Expression,
  override val mutableAggBufferOffset: Int = 0,
  override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[CBM]
    with TernaryLike[Expression] {

  override def first: Expression = bucket

  override def second: Expression = id

  override def third: Expression = count

  override def createAggregationBuffer(): CBM = new CBM()

  override def checkInputDataTypes(): TypeCheckResult = {
    (first.dataType, second.dataType, third.dataType) match {
      case (NullType, _, _) | (_, NullType, _) | (_, _, NullType) =>
        TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as size arguments")
      case (IntegerType, IntegerType, LongType) =>
        TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"Input to function $prettyName should have " +
            s"three arguments with ${IntegerType.simpleString}, ${IntegerType.simpleString} and ${LongType.simpleString}," +
            s"but it's [${first.dataType.catalogString}, " +
            s"${second.dataType.catalogString}, ${third.dataType.catalogString}]"
        )
    }
  }

  override def update(buffer: CBM, input: InternalRow): CBM = {
    val _bucket = bucket.eval(input).asInstanceOf[Int]
    val _id     = id.eval(input).asInstanceOf[Int]
    val _count  = count.eval(input).asInstanceOf[Number].longValue()
    buffer.add(_bucket, _id, _count)
  }

  override def merge(buffer: CBM, input: CBM): CBM = {
    buffer.or(input)
  }

  override def eval(buffer: CBM): Any = {
    if (buffer.isEmpty) {
      null
    } else {
      buffer.getBytes
    }
  }

  override def serialize(buffer: CBM): Array[Byte] = {
    buffer.getBytes
  }

  override def deserialize(storageFormat: Array[Byte]): CBM = {
    new CBM(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override protected def withNewChildrenInternal(
    newFirst: Expression,
    newSecond: Expression,
    newThird: Expression
  ): Expression =
    copy(bucket = newFirst, id = newSecond, count = newThird)

  override def prettyName: String = "cbm_aggr"
}

object CBMAggr {
  def apply(bucket: Expression, uid: Expression, count: Expression) = new CBMAggr(bucket, uid, count, 0, 0)
}
