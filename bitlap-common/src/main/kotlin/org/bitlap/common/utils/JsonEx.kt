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

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

/**
 * json extension utils
 */
object JsonEx {

    val gson = Gson()

    @JvmStatic
    fun Any?.json(): String {
        return gson.toJson(this)
    }

    @JvmStatic
    inline fun <reified T> String.jsonAs(): T {
        return gson.fromJson(this, T::class.java)
    }

    @JvmStatic
    fun <T> String.jsonAs(clazz: Class<T>): T {
        return gson.fromJson(this, clazz)
    }

    @JvmStatic
    fun String?.jsonAsMap(): Map<String, String> {
        if (this.isNullOrBlank()) return emptyMap()
        return gson.fromJson(this, object : TypeToken<Map<String, String>>() {}.type)
    }
}
