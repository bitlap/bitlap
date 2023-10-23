/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

/**
 * bitlap writers utils
 */
object BitlapWriters {

    // FileOutputFormat#getUniqueFile
    def genUniqueFile(txId: String, name: String, extension: String): String = {
        s"$name-$txId$extension"
    }
}
