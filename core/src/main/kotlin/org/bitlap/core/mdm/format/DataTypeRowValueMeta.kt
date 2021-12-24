package org.bitlap.core.mdm.format

import org.bitlap.core.mdm.model.RowValueMeta

class DataTypeRowValueMeta(override val name: String, override val idx: Int) : DataType() {

    override fun defaultValue(): RowValueMeta {
        return RowValueMeta.empty()
    }
}
