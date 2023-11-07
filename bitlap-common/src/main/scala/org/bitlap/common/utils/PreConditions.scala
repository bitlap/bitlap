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
package org.bitlap.common.utils

import org.bitlap.common.exception.BitlapExceptions

object PreConditions {

  /** check [o] cannot be null
   */
  def checkNotNull[T](o: T, key: String = "Object"): T = {
    if (o == null) {
      throw BitlapExceptions.checkNotNullException(key)
    }
    o
  }

  /** check [str] cannot be null or blank
   */
  def checkNotBlank(str: String, key: String = "string"): String = {
    if (str == null || str.isBlank) {
      throw BitlapExceptions.checkNotBlankException(key)
    }
    str
  }

  /** check [collection] cannot be empty
   */
  def checkNotEmpty[T](collection: Seq[T], key: String = "collection"): Seq[T] = {
    if (collection == null || collection.isEmpty) {
      throw BitlapExceptions.checkNotEmptyException(key)
    }
    collection
  }

  /** check [expr] cannot be false
   */
  def checkExpression(expr: Boolean, key: String = "expr", msg: String = s"expression cannot be false"): Unit = {
    if (!expr) {
      throw IllegalArgumentException(msg)
    }
  }
}
