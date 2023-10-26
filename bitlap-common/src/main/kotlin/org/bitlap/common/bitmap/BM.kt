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

import java.io.Externalizable
import java.io.Serializable
import java.nio.ByteBuffer

/**
 * BM
 */
interface BM : Serializable, Externalizable {

    fun clear(): BM
    fun empty(): BM
    fun trim(): BM
    fun isEmpty(): Boolean

    fun repair(): BM
    fun getCount(): Double
    fun getLongCount(): Long
    fun getCountUnique(): Long
    fun getRBM(): RBM
    fun getSizeInBytes(): Long
    fun split(splitSize: Int, copy: Boolean = false): Map<Int, BM>

    /**
     * serialize
     */
    fun getBytes(buffer: ByteBuffer?): ByteArray
    fun getBytes(): ByteArray
    fun setBytes(bytes: ByteArray? = null): BM

    /**
     * operators
     */
    fun and(bm: BM): BM
    fun andNot(bm: BM): BM
    fun or(bm: BM): BM
    fun xor(bm: BM): BM

    fun contains(dat: Int): Boolean
    fun clone(): BM
}
