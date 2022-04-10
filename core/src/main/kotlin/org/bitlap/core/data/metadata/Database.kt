/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.data.metadata

/**
 * Database
 */
data class Database(private val _name: String) {
    val name: String
        get() = _name.lowercase()
}
