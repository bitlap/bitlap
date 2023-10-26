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

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/24
 */
interface ComparableBM : BM {

    /**
     * get [BM] that [BM.getCount] > [threshold]
     */
    fun gt(threshold: Double): ComparableBM
    fun gt(threshold: Double, copy: Boolean): ComparableBM

    /**
     * get [BM] that [BM.getCount] >= [threshold]
     */
    fun gte(threshold: Double): ComparableBM
    fun gte(threshold: Double, copy: Boolean): ComparableBM

    /**
     * get [BM] that [BM.getCount] < [threshold]
     */
    fun lt(threshold: Double): ComparableBM
    fun lt(threshold: Double, copy: Boolean): ComparableBM

    /**
     * get [BM] that [BM.getCount] <= [threshold]
     */
    fun lte(threshold: Double): ComparableBM
    fun lte(threshold: Double, copy: Boolean): ComparableBM

    /**
     * get [BM] that [BM.getCount] = [threshold]
     */
    fun eq(threshold: Double): ComparableBM
    fun eq(threshold: Double, copy: Boolean): ComparableBM

    /**
     * get [BM] that [BM.getCount] != [threshold]
     */
    fun neq(threshold: Double): ComparableBM
    fun neq(threshold: Double, copy: Boolean): ComparableBM

    /**
     * get [BM] that [BM.getCount] between [[first], [second]]
     */
    fun between(first: Double, second: Double): ComparableBM
    fun between(first: Double, second: Double, copy: Boolean): ComparableBM
}
