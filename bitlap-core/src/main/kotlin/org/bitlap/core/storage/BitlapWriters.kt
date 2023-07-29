/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

/**
 * Desc: bitlap writers utils
 */
object BitlapWriters {

    // FileOutputFormat#getUniqueFile
    fun genUniqueFile(txId: String, name: String, extension: String): String {
        return "$name-$txId$extension"
    }
}
