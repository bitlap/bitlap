/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    this.from0(`type`, name, idx)._1
  }

  /** get data type default value
   */
  def defaultValue(`type`: Class[_]): Any = {
    this.from0(`type`, "", 0)._2
  }

  /** reset data type index
   */
  def resetIndex(dataType: DataType, idx: Int): DataType = {
    this.from0(dataType.getClass, dataType.name, idx)._1
  }

  private def from0(`type`: Class[_], name: String, idx: Int): (DataType, Any) = {
    `type` match {
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
