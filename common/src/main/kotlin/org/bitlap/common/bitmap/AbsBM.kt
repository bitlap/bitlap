package org.bitlap.common.bitmap

import java.io.ObjectInput
import java.io.ObjectOutput

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/21
 */
abstract class AbsBM<T> : BM<T> {

    @Volatile protected var modified: Boolean = true
    protected fun <T> resetModify(func: () -> T): T {
        modified = true
        return func.invoke()
    }

    override fun writeExternal(out: ObjectOutput) {
        out.write(this.getBytes())
    }

    override fun readExternal(`in`: ObjectInput) {
        val bytes = ByteArray(`in`.available())
        `in`.readFully(bytes)
        this.setBytes(bytes)
    }
}
