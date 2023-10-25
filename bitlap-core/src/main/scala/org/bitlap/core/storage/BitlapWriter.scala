/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

import java.io.Closeable

/** mdm bitlap writer
 */
trait BitlapWriter[T] extends Closeable {

  def writeBatch(rows: Iterable[T]): Unit

  def write(row: T): Unit
}
