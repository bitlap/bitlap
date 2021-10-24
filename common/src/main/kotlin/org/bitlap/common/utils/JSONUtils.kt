package org.bitlap.common.utils

import com.google.gson.Gson
import kotlin.String

/**
 * Desc: json utils
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/12
 */
object JSONUtils {

    private val gson = Gson()

    @JvmStatic
    fun toJson(src: Any): String {
        return gson.toJson(src)
    }

    @JvmStatic
    fun <T> fromJson(json: String, type: Class<T>): T {
        return gson.fromJson(json, type)
    }
}
