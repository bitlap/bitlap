package org.bitlap.core.mdm.format

import java.io.Serializable

abstract class DataType : Serializable {

    abstract val name: String
    abstract val idx: Int

    abstract fun defaultValue(): Any

    override fun toString(): String {
        return "DataType(name='$name', idx=$idx)"
    }
}
