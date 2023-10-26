/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.bitmap

import java.io.ObjectInput
import java.io.ObjectOutput

/**
 * Abstract of [BM]
 */
abstract class AbsBM : BM {

    @Volatile
    @Transient
    protected var modified: Boolean = true
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
