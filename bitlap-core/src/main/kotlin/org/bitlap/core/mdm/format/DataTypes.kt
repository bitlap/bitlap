/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.format

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.bitmap.RBM
import org.bitlap.core.mdm.model.RowValueMeta

/**
 * [DataType] tools
 */
object DataTypes {

    /**
     * infer data type with input class [T]
     */
    inline fun <reified T> from(name: String, idx: Int = 0): DataType {
        return this.from(T::class.java, name, idx)
    }

    /**
     * infer data type with input class [type]
     */
    fun from(type: Class<*>, name: String, idx: Int): DataType {
        return this.from0(type, name, idx).first
    }

    /**
     * get data type default value
     */
    fun defaultValue(type: Class<*>): Any {
        return this.from0(type, "", 0).second()
    }

    /**
     * reset data type index
     */
    fun resetIndex(dataType: DataType, idx: Int): DataType {
        return this.from0(dataType::class.java, dataType.name, idx).first
    }

    private fun from0(type: Class<*>, name: String, idx: Int): Pair<DataType, () -> Any> {
        return when (type) {
            DataTypeLong::class.java,
            Long::class.java,
            java.lang.Long::class.java -> DataTypeLong(name, idx) to { 0L }
            DataTypeString::class.java,
            String::class.java,
            java.lang.String::class.java -> DataTypeString(name, idx) to { "" }
            DataTypeRowValueMeta::class.java,
            RowValueMeta::class.java -> DataTypeRowValueMeta(name, idx) to { RowValueMeta.empty() }
            DataTypeRBM::class.java, RBM::class.java -> DataTypeRBM(name, idx) to { RBM() }
            DataTypeBBM::class.java, BBM::class.java -> DataTypeBBM(name, idx) to { BBM() }
            DataTypeCBM::class.java, CBM::class.java -> DataTypeCBM(name, idx) to { CBM() }
            else -> throw IllegalArgumentException("Invalid data type: ${type.name}")
        }
    }
}
