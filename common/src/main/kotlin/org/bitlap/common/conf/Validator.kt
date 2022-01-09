package org.bitlap.common.conf

/**
 * Desc: Validator for [BitlapConfKey] value.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
fun interface Validator<T> {

    /**
     * check [t] if is valid.
     */
    fun validate(t: T?): Boolean
}
