package org.bitlap.common

import cn.hutool.setting.Setting
import org.bitlap.common.conf.BitlapConfKey
import org.bitlap.common.conf.Validators
import org.bitlap.common.utils.StringEx.withPaths
import org.slf4j.LoggerFactory
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
open class BitlapConf() : Serializable {

    private val log = LoggerFactory.getLogger(BitlapConf::class.java)

    @Volatile
    private lateinit var sessionConf: Map<String, String>

    // for session
    constructor(sessionConf: Map<String, String>) : this() {
        this.sessionConf = sessionConf
    }

    fun getSessionConfig(): Map<String, String> = this.sessionConf

    fun setSessionConfig(conf: Map<String, String>) {
        this.sessionConf = conf
    }

    /**
     * core properties
     */
    val props by lazy {
        try {
            Setting("bitlap.setting")
        } catch (e: Exception) {
            log.warn("Loading bitlap.setting config error, cause: ${e.message}")
            Setting()
        }
    }

    fun <T> get(key: BitlapConfKey<T>): T? {
        // TODO: Add cache
        return key.getValue(this)
    }

    @Synchronized
    fun set(key: BitlapConfKey<String>, value: String = "", overwrite: Boolean = false): String {
        return this.set(key.group, key.key, value, overwrite)
    }

    @Synchronized
    fun set(group: String, key: String, value: String = "", overwrite: Boolean = false): String {
        var v = this.get(group, key)
        if (v == null || overwrite) {
            this.props.setByGroup(key, "default", value)
            v = value
        }
        return v
    }

    fun get(group: String, key: String): String? = this.props.get(group, key)

    @Synchronized
    fun reload() {
        // TODO
    }

    companion object {
        /**
         * Project name, default is bitlap
         */
        @JvmField
        val PROJECT_NAME = BitlapConfKey("project.name", "bitlap")
            .sys("bitlap.project.name")
            .env("BITLAP_PROJECT_NAME")

        /**
         * Data dir and local dir
         */
        @JvmField
        val DEFAULT_ROOT_DIR_DATA = BitlapConfKey<String>("root.dir.data")
            .validator(Validators.NOT_BLANK)
        @JvmField
        val DEFAULT_ROOT_DIR_LOCAL = BitlapConfKey<String>("root.dir.local")
            .validator(Validators.NOT_BLANK)
        @JvmField
        val DEFAULT_ROOT_DIR_LOCAL_META = BitlapConfKey<String>("root.dir.local.meta")
            .defaultBy {
                it.get(DEFAULT_ROOT_DIR_LOCAL)?.withPaths("meta")
            }

        /**
         * Node address
         */
        @JvmField
        val NODE_BIND_HOST = BitlapConfKey<String>("node.bind.host")
            .validator(Validators.NOT_BLANK)
        @JvmField
        val NODE_BIND_PEERS = BitlapConfKey<String>("node.bind.peers")
            .validator(Validators.NOT_BLANK)
        /**
         * Sofa RPC cluster name.
         */
        @JvmField
        val NODE_GROUP_ID = BitlapConfKey<String>("node.group.id")
            .validator(Validators.NOT_BLANK)

        /**
         * Sofa RPC timeout, Unit: Second.
         */
        @JvmField
        val NODE_RPC_TIMEOUT = BitlapConfKey<String>("node.rpc.timeout")
            .validator(Validators.NOT_BLANK)

        /**
         * Sofa RAFT timeout, Unit: Second.
         */
        @JvmField
        val NODE_RAFT_TIMEOUT = BitlapConfKey<String>("node.raft.timeout")
            .validator(Validators.NOT_BLANK)
    }
}
