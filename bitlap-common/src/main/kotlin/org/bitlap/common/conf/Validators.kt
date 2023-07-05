/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.conf

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object Validators {

    val NOT_BLANK = Validator<String> { t -> !t.isNullOrBlank() }
    fun <T : Comparable<T>> eq(v: T) = Validator<T> { t -> t != null && t.compareTo(v) == 0 }
    fun <T : Comparable<T>> neq(v: T) = Validator<T> { t -> t != null && t.compareTo(v) != 0 }
    fun <T : Comparable<T>> gt(v: T) = Validator<T> { t -> t != null && t > v }
    fun <T : Comparable<T>> gte(v: T) = Validator<T> { t -> t != null && t >= v }
    fun <T : Comparable<T>> lt(v: T) = Validator<T> { t -> t != null && t < v }
    fun <T : Comparable<T>> lte(v: T) = Validator<T> { t -> t != null && t <= v }
}
