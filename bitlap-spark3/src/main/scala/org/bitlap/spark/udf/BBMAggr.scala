package org.bitlap.spark.udf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, NullType}
import org.bitlap.common.bitmap.BBM


/**
 * Collect bucket and uid in each row into a [[BBM]].
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(bucket, uid) - Collect `bucket` and `uid` in each row into a bucket bitmap.
    """
)
case class BBMAggr(
  bucket: Expression,
  id: Expression,
  override val mutableAggBufferOffset: Int = 0,
  override val inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[BBM] with BinaryLike[Expression] {

  def this(bucket: Expression, uid: Expression) = {
    this(bucket, uid, 0, 0)
  }

  override def left: Expression = bucket

  override def right: Expression = id

  override def createAggregationBuffer(): BBM = new BBM()

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (NullType, _) | (_, NullType) =>
        TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as size arguments")
      case (IntegerType, IntegerType) =>
        TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"two arguments with ${IntegerType.simpleString} and ${IntegerType.simpleString}," +
          s"but it's [${left.dataType.catalogString}, ${right.dataType.catalogString}]"
        )
    }
  }

  override def update(buffer: BBM, input: InternalRow): BBM = {
    val _bucket = bucket.eval(input).asInstanceOf[Int].toShort
    val _uid = id.eval(input).asInstanceOf[Int]
    buffer.add(_bucket, _uid)
    buffer
  }

  override def merge(buffer: BBM, input: BBM): BBM = {
    buffer.or(input)
  }

  override def eval(buffer: BBM): Any = {
    if (buffer.isEmpty) {
      null
    } else {
      buffer.getBytes
    }
  }

  override def serialize(buffer: BBM): Array[Byte] = {
    buffer.getBytes
  }

  override def deserialize(storageFormat: Array[Byte]): BBM = {
    new BBM(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)


  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(bucket = newLeft, id = newRight)

  override def prettyName: String = "bbm_aggr"
}

