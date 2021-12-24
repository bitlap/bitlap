package org.bitlap.core.mdm.format

import org.bitlap.common.bitmap.CBM
import org.bitlap.core.mdm.model.RowValueMeta

/**
 * [DataType] tools
 */
object DataTypes {

    /**
     * infer data type with input class [T]
     */
    inline fun <reified T> from(name: String, idx: Int = 0): DataType {
        return when (T::class.java) {
            Long::class.java,
            java.lang.Long::class.java ->
                DataTypeLong(name, idx)
            String::class.java,
            java.lang.String::class.java ->
                DataTypeString(name, idx)
            CBM::class.java ->
                DataTypeCBM(name, idx)
            RowValueMeta::class.java ->
                DataTypeRowValueMeta(name, idx)
            else -> throw IllegalArgumentException("Invalid data type: ${T::class.java.name}")
        }
    }

    /**
     * reset data type index
     */
    fun resetIndex(dataType: DataType, idx: Int): DataType {
        val name = dataType.name
        return when (dataType) {
            is DataTypeRowValueMeta -> DataTypeRowValueMeta(name, idx)
            is DataTypeCBM -> DataTypeCBM(name, idx)
            is DataTypeString -> DataTypeString(name, idx)
            is DataTypeLong -> DataTypeLong(name, idx)
            else -> throw IllegalArgumentException("Invalid data type: $dataType")
        }
    }
}
