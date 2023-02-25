/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.conf

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object Validators {

    val NOT_BLANK = Validator<String> { t -> !t.isNullOrBlank() }
}
