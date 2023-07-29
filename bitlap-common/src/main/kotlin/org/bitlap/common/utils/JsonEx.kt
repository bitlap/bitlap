/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.utils

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

/**
 * Desc: json extension utils
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/12
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
    fun String?.jsonAsMap(): Map<String, String> {
        if (this.isNullOrBlank()) return emptyMap()
        return gson.fromJson(this, object : TypeToken<Map<String, String>>() {}.type)
    }
}
