package org.bitlap.common.conf

import cn.hutool.core.convert.Convert
import cn.hutool.system.SystemUtil
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.PreConditions

/**
 * Desc: Key for [org.bitlap.common.BitlapConf]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/28
 */
inline fun <reified T> BitlapConfKey(key: String, defaultValue: T? = null): BitlapConfKey<T> {
    return BitlapConfKey(key, defaultValue, T::class.java)
}
class BitlapConfKey<T>(val key: String, val defaultValue: T? = null, private val type: Class<T>) {

    /**
     * Property group
     */
    var group = "default"

    /**
     * Assign default value by function
     */
    var defaultBy: (BitlapConf) -> T? = { this.defaultValue }

    /**
     * System property, default is: bitlap.[group].[key]
     */
    var sys = ""
    fun getSysKey(): String {
        if (this.sys.isNotBlank()) {
            return this.sys
        }
        return when (group) {
            "default" -> "bitlap.$key"
            else -> "bitlap.$group.$key"
        }
    }

    /**
     * System environment property, default is: BITLAP_[group]_[key] (uppercase)
     */
    var env = ""
    fun getEnvKey(): String {
        if (this.env.isNotBlank()) {
            return this.env
        }
        val cleanKey = key.replace(".", "_")
        return when (group) {
            "default" -> "bitlap_$cleanKey"
            else -> "bitlap_${group}_$cleanKey"
        }.uppercase()
    }

    var desc = ""
    var version = "1.0.0"
    var validator: Validator<T>? = null

    fun defaultBy(func: (BitlapConf) -> T?): BitlapConfKey<T> = this.also {
        this.defaultBy = func
    }

    fun group(groupName: String): BitlapConfKey<T> = this.also {
        it.group = groupName
    }

    fun sys(systemProperty: String): BitlapConfKey<T> = this.also {
        it.sys = systemProperty
    }

    fun env(envName: String): BitlapConfKey<T> = this.also {
        it.env = envName
    }

    fun desc(description: String): BitlapConfKey<T> = this.also {
        it.desc = description
    }

    fun version(version: String): BitlapConfKey<T> = this.also {
        it.version = version
    }

    fun validator(v: Validator<T>): BitlapConfKey<T> = this.also {
        it.validator = v
    }

    @Suppress("UNCHECKED_CAST")
    fun getValue(conf: BitlapConf): T? {
        var value = conf.get(this.group, this.key)
        if (value == null) {
            value = SystemUtil.get(this.getSysKey(), SystemUtil.get(this.getEnvKey()))
        }

        val result = if (value == null) {
            this.defaultBy(conf)
        } else {
            value = value.trim()
            when (this.type) {
                String::class.java -> value
                Byte::class.java -> Convert.toByte(value)
                Short::class.java -> Convert.toShort(value)
                Int::class.java -> Convert.toInt(value)
                Long::class.java -> Convert.toLong(value)
                Float::class.java -> Convert.toFloat(value)
                Double::class.java -> Convert.toDouble(value)
                Char::class.java -> Convert.toChar(value)
                Boolean::class.java -> {
                    if (value.isBlank()) {
                        false
                    } else {
                        Convert.toBool(value)
                    }
                }
                else -> throw IllegalArgumentException("Illegal value type: ${this.type}")
            } as T?
        }

        if (this.validator != null) {
            PreConditions.checkExpression(this.validator!!.validate(result), msg = "Value of [$key] is invalid.")
        }
        return result
    }
}
