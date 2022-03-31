package org.bitlap.common.conf

import org.bitlap.common.BitlapConf

/**
 * Desc: Key for [org.bitlap.common.BitlapConf]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/28
 */
inline fun <reified T> BitlapConfKey(key: String, defaultValue: T? = null): BitlapConfKey<T> {
    return BitlapConfKey(key, defaultValue, T::class.java).also {
        BitlapConfKey.cache[key] = it
    }
}

class BitlapConfKey<T>(val key: String, val defaultValue: T? = null, val type: Class<T>) {

    companion object {
        val cache = mutableMapOf<String, BitlapConfKey<*>>()
    }

    /**
     * Property group
     */
    // var group = "default"

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
        return key
    }

    /**
     * System environment property, default is: BITLAP_[group]_[key] (uppercase)
     */
    var env = ""
    fun getEnvKey(): String {
        if (this.env.isNotBlank()) {
            return this.env
        }
        return key.replace(".", "_").uppercase()
    }

    var desc = ""
    var version = "1.0.0"
    var validator: Validator<T>? = null
    var overWritable: Boolean = false

    fun defaultBy(func: (BitlapConf) -> T?): BitlapConfKey<T> = this.also { this.defaultBy = func }

//    fun group(groupName: String): BitlapConfKey<T> = this.also {
//        it.group = groupName
//    }

    fun sys(systemProperty: String): BitlapConfKey<T> = this.also { it.sys = systemProperty }

    fun env(envName: String): BitlapConfKey<T> = this.also { it.env = envName }

    fun desc(description: String): BitlapConfKey<T> = this.also { it.desc = description }

    fun version(version: String): BitlapConfKey<T> = this.also { it.version = version }

    fun validator(v: Validator<T>): BitlapConfKey<T> = this.also { it.validator = v }

    fun overWritable(o: Boolean): BitlapConfKey<T> = this.also { it.overWritable = o }

    override fun toString(): String {
        return "BitlapConfKey(key='$key', defaultValue=$defaultValue, type=$type, sys='$sys', env='$env', desc='$desc', version='$version')"
    }
}
