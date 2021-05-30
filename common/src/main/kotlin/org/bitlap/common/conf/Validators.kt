package org.bitlap.common.conf

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object Validators {

    val NOT_BLANK = object : Validator<String> {
        override fun validate(t: String?): Boolean {
            return !t.isNullOrBlank()
        }
    }
}
