package org.bitlap.core.model.query

/**
 * Desc: Time for qeury
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/19
 */
data class Time(
    val start: Long,
    val end: Long? = null,
    val include: Pair<Boolean, Boolean> = true to true
) {
    private val hasEnd = end != null
}
