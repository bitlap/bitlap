package org.bitlap.core.storage

import java.io.Closeable

/** mdm bitlap reader
 */
trait BitlapReader[T] extends Closeable with Iterator[T] {
  def read(): T
  def read(limit: Int): List[T]
}
