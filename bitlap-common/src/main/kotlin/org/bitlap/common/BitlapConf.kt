/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common

import cn.hutool.core.convert.Convert
import cn.hutool.setting.Setting
import cn.hutool.system.SystemUtil
import org.bitlap.common.conf.BitlapConfKey
import org.bitlap.common.conf.Validators
import org.bitlap.common.utils.PreConditions
import java.io.Serializable

/**
 * Desc: Bitlap core configuration.
 *
 * [BitlapConfKey] is designed as follows, and the priority is the same as below:
 *   1. with `name` and `group` to get value from `bitlap.setting` configuration
 *   2. with `sys` to get value from system properties, default is `bitlap.${name}`
 *   3. with `env` to get value from system environment, default is `BITLAP_${upper_trans_dot(name)}`
 *   4. `default` value
 *   5. value `data type`
 *   6. value `validator`
 *   7. conf key `version`
 *   8. ......
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/28
 */
open class BitlapConf(private val conf: Map<String, String> = emptyMap()) : Serializable {

    private val log = logger { }

    /**
     * core properties
     */
    private val props by lazy {
        try {
            Setting("bitlap.setting")
        } catch (e: Exception) {
            log.warn("Loading bitlap.setting config error, cause: ${e.message}")
            Setting()
        }
    }

    init {
        // merge props
        this.conf.forEach { (key, value) ->
            this.set(key, value)
        }
    }

    @Synchronized
    fun set(key: String, value: String = "", forceOverwrite: Boolean = false): String {
        val confKey = BitlapConfKey.cache[key]
        if (confKey != null) {
            if (confKey.overWritable || forceOverwrite) {
                this.props.set(key, value)
            } else {
                throw IllegalArgumentException("$key cannot be overwrite.")
            }
        } else {
            this.props.set(key, value)
        }
        return value
    }

    fun getString(key: String): String? {
        val confKey = BitlapConfKey.cache[key]
        if (confKey != null) {
            return this.get(confKey)?.toString()
        }
        return this.props[key]
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> get(key: BitlapConfKey<T>): T? {
        // TODO: Add cache
        var value = this.props[key.key]
        if (value == null) {
            value = SystemUtil.get(key.getSysKey(), SystemUtil.get(key.getEnvKey()))
        }

        val result = if (value == null) {
            key.defaultBy(this)
        } else {
            value = value.trim()
            when (key.type) {
                String::class.java -> value
                Byte::class.java -> Convert.toByte(value)
                Short::class.java -> Convert.toShort(value)
                Int::class.java -> Convert.toInt(value)
                Long::class.java, java.lang.Long::class.java -> Convert.toLong(value)
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
                else -> throw IllegalArgumentException("Illegal value type: ${key.type}")
            } as T?
        }

        if (key.validator != null) {
            PreConditions.checkExpression(key.validator!!.validate(result), msg = "Value of [$key] is invalid.")
        }
        return result
    }

    @Synchronized
    fun reload() {
        // TODO
    }

    companion object {
        /**
         * Project name, default is bitlap
         */
        @JvmField
        val PROJECT_NAME = BitlapConfKey("bitlap.project.name", "bitlap")
            .sys("bitlap.project.name")
            .env("BITLAP_PROJECT_NAME")

        /**
         * Data dir FIXME 不加默认有问题
         */
        @JvmField
        val ROOT_DIR_DATA = BitlapConfKey<String>("bitlap.root.dir.data", "/tmp/bitlap_data").validator(Validators.NOT_BLANK)

        /**
         * Local dir
         */
        @JvmField
        val ROOT_DIR_LOCAL = BitlapConfKey<String>("bitlap.root.dir.local").validator(Validators.NOT_BLANK)

        /**
         * Node address, Rpc configuration
         */
        @JvmField
        val NODE_BIND_HOST = BitlapConfKey<String>("bitlap.node.bind.host", "127.0.0.1:23333").validator(Validators.NOT_BLANK)

        @JvmField
        val NODE_BIND_PEERS = BitlapConfKey<String>("bitlap.node.bind.peers")
            .defaultBy { it.get(NODE_BIND_HOST) }

        @JvmField
        val NODE_GROUP_ID = BitlapConfKey("bitlap.node.group.id", "bitlap")
            .validator(Validators.NOT_BLANK)

        /**
         * timeout in milliseconds
         */
        @JvmField
        val NODE_RPC_TIMEOUT = BitlapConfKey("bitlap.node.rpc.timeout", 3000L)
            .overWritable(true)
            .validator { it != null && it >= 1000L }

        @JvmField
        val NODE_READ_TIMEOUT = BitlapConfKey("bitlap.node.read.timeout", 10000L)
            .validator { it != null && it >= 1000L }

        @JvmField
        val RAFT_DATA_PATH = BitlapConfKey<String>("bitlap.node.raft.data", "/tmp/server/bitlap_raft").validator(Validators.NOT_BLANK)

        @JvmField
        val RAFT_SERVER_ADDRESS = BitlapConfKey<String>("bitlap.node.raft.host", "127.0.0.1:12222").validator(Validators.NOT_BLANK)

        @JvmField
        val RAFT_INITIAL_SERVER_ADDRESS = BitlapConfKey<String>("bitlap.node.raft.initialServerAddress", "127.0.0.1:12222").validator(Validators.NOT_BLANK)

        @JvmField
        val RAFT_TIMEOUT = BitlapConfKey<String>("bitlap.node.raft.timeout", "10s").validator(Validators.NOT_BLANK)

        @JvmField
        val HTTP_SERVER_ADDRESS = BitlapConfKey<String>("bitlap.node.http.host", "127.0.0.1:18081").validator(Validators.NOT_BLANK)

        @JvmField
        val SESSION_TIMEOUT = BitlapConfKey<String>("bitlap.node.session.timeout", "20m").validator(Validators.NOT_BLANK)
    }
}
