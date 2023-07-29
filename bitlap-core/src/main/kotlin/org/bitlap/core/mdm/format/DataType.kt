/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.format

import java.io.Serializable

abstract class DataType : Serializable {

    abstract val name: String
    abstract val idx: Int

    override fun toString(): String {
        return "$name#$idx"
    }
}
