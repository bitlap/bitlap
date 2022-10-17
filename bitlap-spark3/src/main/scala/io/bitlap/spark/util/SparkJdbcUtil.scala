/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.util
import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.util.{ DateTimeUtils, GenericArrayData }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{ PreparedStatement, ResultSet }

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/17
 */
object SparkJdbcUtil {

  def toRow(encoder: ExpressionEncoder[Row], internalRow: InternalRow): Row =
    encoder.createDeserializer().apply(internalRow)

  // A `JDBCValueGetter` is responsible for getting a value from `ResultSet` into a field
  // for `MutableRow`. The last argument `Int` means the index for the value to be set in
  // the row and also used for the value in `ResultSet`.
  private type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit

  private def nullSafeConvert[T](input: T, f: T => Any): Any =
    if (input == null) {
      null
    } else {
      f(input)
    }

  /** Creates `JDBCValueGetter`s according to [[StructType]], which can set each value from `ResultSet` to each field of
   *  [[InternalRow]] correctly.
   */
  private def makeGetters(schema: StructType): Array[JDBCValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))

  private object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] = Some((t.precision, t.scale))
  }

  private def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
        } else {
          row.update(pos, null)
        }

      // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
      // object returned by ResultSet.getBigDecimal is not correctly matched to the table
      // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
      // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
      // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
      // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
      // retrieve it, you will get wrong result 199.99.
    // So it is needed to set precision and scale for Decimal based on JDBC metadata.
    case Fixed(p, s) =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val decimal =
          nullSafeConvert[java.math.BigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        var ans   = 0L
        var j     = 0
        while (j < bytes.length) {
          ans = 256 * ans + (255 & bytes(j))
          j = j + 1
        }
        row.setLong(pos, ans)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setShort(pos, rs.getShort(pos + 1))

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.update(pos, rs.getBytes(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.update(pos, rs.getByte(pos + 1))

    case ArrayType(et, _) =>
      val elementConversion = et match {
        case TimestampType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
              nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
            }

        case StringType =>
          (array: Object) =>
            // some underling types are not String such as uuid, inet, cidr, etc.
            array
              .asInstanceOf[Array[java.lang.Object]]
              .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

        case DateType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Date]].map { date =>
              nullSafeConvert(date, DateTimeUtils.fromJavaDate)
            }

        case dt: DecimalType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
              nullSafeConvert[java.math.BigDecimal](decimal, d => Decimal(d, dt.precision, dt.scale))
            }

        case LongType if metadata.contains("binarylong") =>
          throw new IllegalArgumentException(
            s"Unsupported array element " +
              s"type ${dt.catalogString} based on binary"
          )

        case ArrayType(_, _) =>
          throw new IllegalArgumentException("Nested arrays unsupported")

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val array = nullSafeConvert[java.sql.Array](
          input = rs.getArray(pos + 1),
          array => new GenericArrayData(elementConversion.apply(array.getArray))
        )
        row.update(pos, array)

    case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.catalogString}")
  }

  // A `JDBCValueSetter` is responsible for setting a value from `Row` into a field for
  // `PreparedStatement`. The last argument `Int` means the index for the value to be set
  // in the SQL statement and also used for the value in `Row`.
  private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

  abstract class BitlapNextIterator[U] extends Iterator[U] {

    private var gotNext      = false
    private var nextValue: U = _
    private var closed       = false
    protected var finished   = false

    /** Method for subclasses to implement to provide the next element.
     *
     *  If no next element is available, the subclass should set `finished` to `true` and may return any value (it will
     *  be ignored).
     *
     *  This convention is required because `null` may be a valid value, and using `Option` seems like it might create
     *  unnecessary Some/None instances, given some iterators might be called in a tight loop.
     *
     *  @return
     *    U, or set 'finished' when done
     */
    protected def getNext(): U

    /** Method for subclasses to implement when all elements have been successfully iterated, and the iteration is done.
     *
     *  <b>Note:</b> `NextIterator` cannot guarantee that `close` will be called because it has no control over what
     *  happens when an exception happens in the user code that is calling hasNext/next.
     *
     *  Ideally you should have another try/catch, as in HadoopRDD, that ensures any resources are closed should
     *  iteration fail.
     */
    protected def close(): Unit

    /** Calls the subclass-defined close method, but only once.
     *
     *  Usually calling `close` multiple times should be fine, but historically there have been issues with some
     *  InputFormats throwing exceptions.
     */
    def closeIfNeeded(): Unit =
      if (!closed) {
        // Note: it's important that we set closed = true before calling close(), since setting it
        // afterwards would permit us to call close() multiple times if close() threw an exception.
        closed = true
        close()
      }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext()
          if (finished) {
            closeIfNeeded()
          }
          gotNext = true
        }
      }
      !finished
    }

    override def next(): U = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }

  // TODO just use JdbcUtils.resultSetToSparkInternalRows in Spark 3.0 (see SPARK-26499)
  def resultSetToSparkInternalRows(
    resultSet: ResultSet,
    schema: StructType,
    inputMetrics: InputMetrics
  ): Iterator[InternalRow] =
    // JdbcUtils.resultSetToSparkInternalRows(resultSet, schema, inputMetrics)
    new BitlapNextIterator[InternalRow] {
      private[this] val rs                              = resultSet
      private[this] val getters: Array[JDBCValueGetter] = makeGetters(schema)
      private[this] val mutableRow                      = new SpecificInternalRow(schema.fields.map(x => x.dataType))

      override protected def close(): Unit =
        try
          rs.close()
        catch {
          case e: Exception => e.printStackTrace()
        }

      override protected def getNext(): InternalRow =
        if (rs.next()) {
          //          inputMetrics.incRecordsRead(1)
          val m = classOf[InputMetrics].getDeclaredMethod("incRecordsRead", classOf[Long])
          m.setAccessible(true)
          m.invoke(inputMetrics, 1)
          var i = 0
          while (i < getters.length) {
            getters(i).apply(rs, mutableRow, i)
            if (rs.wasNull) mutableRow.setNullAt(i)
            i = i + 1
          }
          mutableRow
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
    }
}
