/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.exception

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/15
 */
open class BitlapException @JvmOverloads constructor(msg: String, cause: Throwable? = null) : RuntimeException(msg, cause)

open class SQLExecutedException @JvmOverloads constructor(msg: String, cause: Throwable? = null) : BitlapException(msg, cause)
