/** Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.format

import java.io.Serializable

abstract class DataType extends Serializable {

  val name: String
  val idx: Int

  override def toString: String = {
    return s"$name#$idx"
  }
}
