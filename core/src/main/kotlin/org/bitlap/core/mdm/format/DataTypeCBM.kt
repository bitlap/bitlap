package org.bitlap.core.mdm.format

import org.bitlap.common.bitmap.CBM

class DataTypeCBM(override val name: String, override val idx: Int) : DataType() {

    override fun defaultValue(): CBM {
        return CBM()
    }
}
