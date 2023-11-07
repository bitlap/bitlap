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
package org.bitlap.core.storage

import org.bitlap.common.BitlapBatchIterator
import org.bitlap.common.utils.PreConditions

/** Batch iterator for metric row
 */
open class BitlapReaderIterator[R](private val reader: BitlapReader[R], private val limit: Int)
    extends BitlapBatchIterator[R]() {

  @volatile
  private var closed = false

  override def nextBatch(): Iterator[R] = {
    this.checkOpen()
    if (this.reader.hasNext) {
      this.reader.read(limit).iterator
    } else {
      null
    }
  }

  override def hasNext(): Boolean = {
    this.checkOpen()
    val has = super.hasNext()
    // auto close
    if (!has) {
      this.close()
    }
    has
  }

  override def next(): R = {
    this.checkOpen()
    super.next()
  }

  override def close(): Unit = {
    if (closed) {
      return
    }
    closed = true
    scala.util.Using.resource(this.reader) { _ => }
  }

  private def checkOpen(): Unit = {
    PreConditions.checkExpression(!closed, "", "BitlapReaderIterator has been closed.")
  }
}
