/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

import java.io.Closeable

/**
 * Desc: mdm bitlap writer
 */
interface BitlapWriter<T> : Closeable {

    fun writeBatch(rows: Iterable<T>)

    fun write(row: T)
}
