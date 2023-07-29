/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.io

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterPredicate
import java.nio.ByteBuffer
import java.util.Arrays

/**
 * get default value [default] if the [key] does not exist.
 */
fun <T> GenericRecord?.getWithDefault(key: String, default: T): T {
    if (this == null || !this.hasField(key)) return default
    val value = this.get(key)
    return when {
        value is Utf8 -> value.toString()
        value is ByteBuffer && value.hasArray() -> {
            Arrays.copyOfRange(value.array(), value.position(), value.limit())
        }

        value is ByteBuffer -> {
            val oldPosition = value.position()
            value.position(0)
            val size = value.limit()
            val buffers = ByteArray(size)
            value.get(buffers)
            value.position(oldPosition)
            buffers
        }

        else -> this.get(key)
    } as T
}

fun FilterPredicate?.compact(): FilterCompat.Filter {
    if (this == null) return FilterCompat.NOOP
    return FilterCompat.get(this)
}
