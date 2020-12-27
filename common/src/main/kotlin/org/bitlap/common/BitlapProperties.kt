package org.bitlap.common

import cn.hutool.setting.Setting
import org.bitlap.common.utils.PreConditions
import java.io.Serializable

/**
 * Desc: Bitlap core properties configuration.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/20
 */
object BitlapProperties : Serializable {

    /**
     * core properties
     */
    private var props = try { Setting("bitlap.setting") } catch (e: Exception) { Setting() }

    /**
     * core properties keys name
     */
    // Bitlap root dir
    const val DEFAULT_ROOT_DIR = "root.dir"

    init {
        PreConditions.checkNotBlank(this.setDefault(DEFAULT_ROOT_DIR), DEFAULT_ROOT_DIR)
    }

    fun setDefault(key: String, value: String = "", overwrite: Boolean = false): String {
        var v = this.getDefault(key)
        if (v == null || overwrite) {
            this.props.set("default", key, value)
            v = value
        }
        return v
    }

    fun getDefault(key: String): String? = this.props.get("default", key)

    /**
     * get root dir
     */
    fun getRootDir(): String = getDefault(DEFAULT_ROOT_DIR)!!
}

