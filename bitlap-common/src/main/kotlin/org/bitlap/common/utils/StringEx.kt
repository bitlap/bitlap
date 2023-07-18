/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.utils

import java.io.File
import java.util.UUID

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/30
 */
object StringEx {

    /**
     * Fix path, concat [subPaths] with [File.pathSeparator]
     */
    @JvmStatic
    fun String.withPaths(vararg subPaths: String): String {
        val separator = File.separator
        return subPaths.fold(this) { p1, p2 ->
            when {
                p2.isBlank() -> p1
                p1.endsWith(separator) && p2.startsWith(separator) -> p1 + p2.substring(1)
                p1.endsWith(separator) || p2.startsWith(separator) -> p1 + p2
                else -> p1 + separator + p2
            }
        }
    }

    /**
     * trim chars
     */
    @JvmStatic
    fun String?.trimMargin(vararg ch: Char): String {
        return this?.trim(*ch) ?: ""
    }

    /**
     * check string is null or blank
     */
    @JvmStatic
    fun String?.nullOrBlank(): Boolean {
        return this.isNullOrBlank()
    }

    /**
     * get [default] if this is blank
     */
    @JvmStatic
    fun String?.blankOr(default: String): String {
        if (this.isNullOrBlank()) {
            return default
        }
        return this
    }

    @JvmStatic
    @JvmOverloads
    fun uuid(removeDash: Boolean = false): String {
        val uuid = UUID.randomUUID().toString()
        if (removeDash) {
            uuid.replace("-", "")
        }
        return uuid
    }
}
