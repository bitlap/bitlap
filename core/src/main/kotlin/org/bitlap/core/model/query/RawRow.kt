package org.bitlap.core.model.query

import org.bitlap.core.reader.DefaultBitlapReader
import java.io.Serializable
import java.util.*

/**
 * Desc: raw data row for [DefaultBitlapReader]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/30
 */
class RawRow(val data: Array<*>) : Serializable {

    override fun toString(): String {
        return Arrays.toString(data)
    }
}
