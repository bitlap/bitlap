package org.bitlap.core.mdm.format

class DataTypeString(override val name: String, override val idx: Int) : DataType() {

    override fun defaultValue(): String {
        return ""
    }
}
