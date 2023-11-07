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
    private val splits                    = rows.grouped(batchSize).iterator
    override def nextBatch(): Iterator[R] = if (splits.hasNext) splits.next().iterator else null
  }
}

abstract class BitlapBatchIterator[+A] extends BitlapIterator[A] {

  private var count                  = 0
  private var end                    = false
  private var rows: Iterator[_ <: A] = _

  /** @return
   *    null if there is no next batch
   */
  protected def nextBatch(): Iterator[A]

  override def hasNext(): Boolean = {
    this.initBatch()
    rows != null && rows.hasNext
  }

  override def next(): A = {
    this.initBatch()
    if (rows != null && rows.hasNext) {
      count += 1
      val row = rows.next()
      if (!rows.hasNext) {
        rows = null
        this.initBatch()
      }
      return row
    }
    throw new NoSuchElementException()
  }

  private def initBatch(): Unit = {
    if (rows == null && !end) {
      rows = this.nextBatch()
      if (rows == null) end = true
    }
  }
}
