/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.format

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.bitmap.RBM
import org.bitlap.core.mdm.model.RowValueMeta

/** [DataType] tools
 */
object DataTypes {

  /** infer data type with input class [type]
   */
  def from(`type`: Class[_], name: String, idx: Int): DataType = {
    return this.from0(`type`, name, idx)._1
  }

  /** get data type default value
   */
  def defaultValue(`type`: Class[_]): Any = {
    return this.from0(`type`, "", 0)._2
  }

  /** reset data type index
   */
  def resetIndex(dataType: DataType, idx: Int): DataType = {
    return this.from0(dataType.getClass, dataType.name, idx)._1
  }

  private def from0(`type`: Class[_], name: String, idx: Int): (DataType, Any) = {
    return `type` match {
      case c if c == classOf[DataTypeLong] || c == classOf[Long] || c == classOf[java.lang.Long] =>
        DataTypeLong(name, idx) -> 0L
      case c if c == classOf[DataTypeString] || c == classOf[String] => DataTypeString(name, idx) -> ""
      case c if c == classOf[DataTypeRowValueMeta] || c == classOf[RowValueMeta] =>
        DataTypeRowValueMeta(name, idx) -> RowValueMeta.empty()
      case c if c == classOf[DataTypeRBM] || c == classOf[RBM] => DataTypeRBM(name, idx) -> RBM()
      case c if c == classOf[DataTypeBBM] || c == classOf[BBM] => DataTypeBBM(name, idx) -> BBM()
      case c if c == classOf[DataTypeCBM] || c == classOf[CBM] => DataTypeCBM(name, idx) -> CBM()
      case _ => throw IllegalArgumentException(s"Invalid data type: ${`type`.getName}")
    }
  }
}
