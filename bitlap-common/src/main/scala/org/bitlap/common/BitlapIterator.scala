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
package org.bitlap.common

import java.io.Closeable

/** Common iterator
 */
abstract class BitlapIterator[+A] extends Iterator[A] with Closeable {

  protected def initialize(): Unit = {
    // ignore
  }

  override def close(): Unit = {
    // ignore
  }

}

object BitlapIterator {

  def empty[R](): BitlapIterator[R] = new BitlapIterator[R] {
    override def next(): R          = null.asInstanceOf[R] // should never be here
    override def hasNext(): Boolean = false
  }

  def of[R](rows: Iterable[R]): BitlapIterator[R] = new BitlapIterator[R] {
    private val it                  = rows.iterator
    override def next(): R          = it.next
    override def hasNext(): Boolean = it.hasNext
  }

  def batch[R](rows: Iterable[R], batchSize: Int = 100): BitlapBatchIterator[R] = new BitlapBatchIterator[R] {
    private val splits                = rows.grouped(batchSize).iterator
    override def hasNext(): Boolean   = splits.hasNext || super.hasNext()
    override def nextBatch(): List[R] = splits.next.toList
  }
}

abstract class BitlapBatchIterator[+A] extends BitlapIterator[A] {

  private var count              = 0
  private var index              = 0
  private var rows: List[_ <: A] = _

  protected def nextBatch(): List[A]

  override def hasNext(): Boolean = {
    rows != null && index < rows.size
  }

  override def next(): A = {
    if (rows == null || index >= rows.size) {
      rows = this.nextBatch()
      index = 0
    }
    val row = rows(index)
    count += 1
    index += 1
    row
  }
}
