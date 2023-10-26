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
