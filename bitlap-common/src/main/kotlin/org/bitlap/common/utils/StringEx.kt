/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
