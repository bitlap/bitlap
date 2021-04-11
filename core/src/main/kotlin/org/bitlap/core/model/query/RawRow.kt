package org.bitlap.core.model.query

import org.bitlap.core.reader.DefaultBitlapReader

/**
 * Desc: raw data row for [DefaultBitlapReader]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/30
 */
data class RawRow(
    val metrics: Map<String, Any> = mutableMapOf(),
    val dimensions: Map<String, String> = mutableMapOf()
)
