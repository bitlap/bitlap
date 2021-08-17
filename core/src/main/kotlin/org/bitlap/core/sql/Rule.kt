package org.bitlap.core.sql

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/11
 */
interface Rule<R> {

    val name: String get() = this::class.java.name

    fun execute(plan: R): R
}
