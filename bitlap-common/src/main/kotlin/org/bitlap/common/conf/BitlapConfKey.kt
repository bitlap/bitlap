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
package org.bitlap.common.conf

import org.bitlap.common.BitlapConf

/**
 * Key for [[org.bitlap.common.BitlapConf]]
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
     * Assign default value by function
     */
    var defaultBy: (BitlapConf) -> T? = { this.defaultValue }

    /**
     * System property, default is: bitlap.[key]
     */
    var sys = ""
    fun getSysKey(): String {
        if (this.sys.isNotBlank()) {
            return this.sys
        }
        return key
    }

    /**
     * System environment property, default is: BITLAP_[key] (uppercase)
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
    var validators: MutableList<Validator<T>> = mutableListOf()
    var overWritable: Boolean = false

    fun defaultBy(func: (BitlapConf) -> T): BitlapConfKey<T> = this.also { this.defaultBy = func }

    fun sys(systemProperty: String): BitlapConfKey<T> = this.also { it.sys = systemProperty }

    fun env(envName: String): BitlapConfKey<T> = this.also { it.env = envName }

    fun desc(description: String): BitlapConfKey<T> = this.also { it.desc = description }

    fun version(version: String): BitlapConfKey<T> = this.also { it.version = version }

    fun validator(v: Validator<T>): BitlapConfKey<T> = this.also { it.validators.add(v) }

    fun validators(vararg v: Validator<T>): BitlapConfKey<T> = this.also { it.validators.addAll(v) }

    fun overWritable(o: Boolean): BitlapConfKey<T> = this.also { it.overWritable = o }

    override fun toString(): String {
        return "BitlapConfKey(key='$key', defaultValue=$defaultValue, type=$type, sys='$sys', env='$env', desc='$desc', version='$version')"
    }
}
