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
package org.bitlap.common

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigParseOptions
import com.typesafe.config.ConfigResolveOptions
import org.bitlap.common.conf.BitlapConfKey
import org.bitlap.common.utils.JsonEx.json
import org.bitlap.common.utils.PreConditions
import java.io.Serializable
import kotlin.time.Duration

/**
 * Bitlap core configuration.
 *
 * [[BitlapConfKey] is designed as follows, and the priority is the same as below:
 *   1. with `sys` to get value from java system properties, default is `bitlap.${name}`
 *   2. with `env` to get value from OS environment variables, default is `BITLAP_${upper_trans_dot(name)}`
 *   3. with `name` to get value from `bitlap.conf` configuration
 *   4. `default` value
 *   5. value `data type`
 *   6. value `validator`
 *   7. conf key `version`
 *   8. ......
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/28
 */
open class BitlapConf(conf: Map<String, String> = emptyMap()) : Serializable {

    private val log = logger { }

    /**
     * core properties
     */
    private val props = run {
        val config = try {
            ConfigFactory.load("bitlap", ConfigParseOptions.defaults(), ConfigResolveOptions.defaults())
        } catch (e: Exception) {
            log.warn("Loading bitlap.conf error, cause: ${e.message}")
            ConfigFactory.empty()
        }
        val map = config.entrySet()
            .filter { it.key.startsWith("bitlap.") }
            .filter { it.value.unwrapped() != null }
            .associate { it.key to it.value.unwrapped().toString() }
            .toMutableMap()
        // load java system properties
        System.getProperties()
            .filter { it.key.toString().startsWith("bitlap.") }
            .forEach {
                map[it.key.toString()] = it.value.toString()
            }
        // load OS environment variables
        System.getenv()
            .filter { it.key.toString().startsWith("BITLAP_") }
            .forEach {
                val key = it.key.replace("_", ".").lowercase()
                map[key] = it.value
            }
        map
    }

    init {
        // force merge props
        conf.forEach { (key, value) ->
            this.set(key, value, true)
        }
    }

    @Synchronized
    fun set(key: String, value: String = "", forceOverwrite: Boolean = false): String {
        val confKey = BitlapConfKey.cache[key]
        if (confKey != null) {
            if (confKey.overWritable || forceOverwrite) {
                this.props[key] = value
            } else {
                throw IllegalArgumentException("$key cannot be overwrite.")
            }
        } else {
            this.props[key] = value
        }
        return value
    }

    @JvmOverloads
    fun clone(other: Map<String, String> = emptyMap()): BitlapConf {
        val thisConf = BitlapConf(this.props)
        other.forEach { (k, v) -> thisConf.set(k, v) }
        return thisConf
    }

    fun toJson(): String {
        return this.props.json()
    }

    override fun toString(): String {
        return this.props.toString()
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> get(confKey: BitlapConfKey<T>): T? {
        val value = this.props[confKey.key]?.trim()
            ?: return confKey.defaultBy(this)

        val result = when (confKey.type) {
            String::class.java -> value
            Byte::class.java, java.lang.Byte::class.java -> value.toByteOrNull()
            Short::class.java, java.lang.Short::class.java -> value.toShortOrNull()
            Int::class.java, java.lang.Integer::class.java -> value.toIntOrNull()
            Long::class.java, java.lang.Long::class.java -> value.toLongOrNull()
            Float::class.java, java.lang.Float::class.java -> value.toFloatOrNull()
            Double::class.java, java.lang.Double::class.java -> value.toDoubleOrNull()
            Char::class.java, java.lang.Character::class.java -> value.firstOrNull()
            Boolean::class.java, java.lang.Boolean::class.java -> value.toBoolean()
            Duration::class.java -> Duration.parse(value)
            else -> throw IllegalArgumentException("Illegal value type: ${confKey.type}")
        } as T?

        confKey.validators.forEach { validator ->
            PreConditions.checkExpression(validator.validate(result), msg = "Value of [$confKey] is invalid.")
        }
        return result
    }

    /**
     * get milliseconds from a duration config
     */
    fun getMillis(confKey: BitlapConfKey<Duration>): Long? {
        return this.get(confKey)?.inWholeMilliseconds
    }

    @Synchronized
    fun reload() {
        // TODO
    }
}
