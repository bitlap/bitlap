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
class BitlapConfKey<T>(val key: String, val defaultValue: T? = null) {

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
        }.toUpperCase()
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

    inline fun <reified R : T> getValue(conf: BitlapConf): R? {
        var value = conf.get(this.group, this.key)
        if (value == null) {
            value = SystemUtil.get(this.getSysKey(), SystemUtil.get(this.getEnvKey()))
        }

        val result = if (value == null) {
            (value ?: this.defaultBy(conf)) as R?
        } else {
            when (R::class) {
                String::class -> value
                Byte::class -> Convert.toByte(value.trim())
                Short::class -> Convert.toShort(value.trim())
                Int::class -> Convert.toInt(value.trim())
                Long::class -> Convert.toLong(value.trim())
                Float::class -> Convert.toFloat(value.trim())
                Double::class -> Convert.toDouble(value.trim())
                Char::class -> Convert.toChar(value.trim())
                Boolean::class -> {
                    if (value.isNullOrBlank()) {
                        false
                    } else {
                        Convert.toBool(value.trim())
                    }
                }
                else -> throw IllegalArgumentException("Illegal value type: ${R::class}")
            } as R?
        }

        if (this.validator != null) {
            PreConditions.checkExpression(this.validator!!.validate(result), msg = "Value of [$key] is invalid.")
        }
        return result
    }
}
