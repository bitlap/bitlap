package org.bitlap.common

import cn.hutool.setting.Setting
import org.bitlap.common.conf.BitlapConfKey
import org.bitlap.common.conf.Validators
import org.bitlap.common.utils.withPaths
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
open class BitlapConf : Serializable {

    private val log = logger { }

    /**
     * core properties
     */
    val props by lazy {
        try {
            Setting("bitlap.setting")
        } catch (e: Exception) {
            log.warn("Loading bitlap.setting config error, cause: ", e)
            Setting()
        }
    }

    inline fun <reified T> get(key: BitlapConfKey<T>): T? {
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
        val PROJECT_NAME = BitlapConfKey("project.name", "bitlap")
            .sys("bitlap.project.name")
            .env("BITLAP_PROJECT_NAME")

        /**
         * Data dir and local dir
         */
        val DEFAULT_ROOT_DIR_DATA = BitlapConfKey<String>("root.dir.data")
            .validator(Validators.NOT_BLANK)
        val DEFAULT_ROOT_DIR_LOCAL = BitlapConfKey<String>("root.dir.local")
            .validator(Validators.NOT_BLANK)
        val DEFAULT_ROOT_DIR_LOCAL_META = BitlapConfKey<String>("root.dir.local.meta")
            .defaultBy {
                it.get(DEFAULT_ROOT_DIR_LOCAL)?.withPaths("meta")
            }

        /**
         * Node address
         */
        val NODE_BIND_HOST = BitlapConfKey<String>("node.bind.host")
            .validator(Validators.NOT_BLANK)
        val NODE_BIND_PEERS = BitlapConfKey<String>("node.bind.peers")
            .validator(Validators.NOT_BLANK)
    }
}
