package org.bitlap.core.mdm.format

class DataTypeLong(override val name: String, override val idx: Int) : DataType() {

    override fun defaultValue(): Long {
        return 0L
    }
}
