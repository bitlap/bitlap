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

import org.bitlap.common.exception.BitlapExceptions

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/20
 */
object PreConditions {

    /**
     * check [o] cannot be null
     */
    @JvmStatic
    @JvmOverloads
    fun <T> checkNotNull(o: T?, key: String = "Object"): T {
        if (o == null) {
            throw BitlapExceptions.checkNotNullException(key)
        }
        return o
    }

    /**
     * check [str] cannot be null or blank
     */
    @JvmStatic
    @JvmOverloads
    fun checkNotBlank(str: String?, key: String = "string"): String {
        if (str.isNullOrBlank()) {
            throw BitlapExceptions.checkNotBlankException(key)
        }
        return str
    }

    /**
     * check [collection] cannot be empty
     */
    @JvmStatic
    @JvmOverloads
    fun <T> checkNotEmpty(collection: Collection<T>?, key: String = "collection"): Collection<T> {
        if (collection.isNullOrEmpty()) {
            throw BitlapExceptions.checkNotEmptyException(key)
        }
        return collection
    }

    /**
     * check [expr] cannot be false
     */
    @JvmStatic
    @JvmOverloads
    fun checkExpression(expr: Boolean, key: String = "expr", msg: String = "$key cannot be false") {
        if (!expr) {
            throw IllegalArgumentException(msg)
        }
    }
}
