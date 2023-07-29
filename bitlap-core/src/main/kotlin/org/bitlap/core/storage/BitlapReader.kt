/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

import java.io.Closeable

/**
 * Desc: mdm bitlap reader
 */
interface BitlapReader<T> : Closeable, Iterator<T> {
    fun read(): T?
    fun read(limit: Int): List<T>
}
