/**
 * Copyright (C) 2023 bitlap.org .
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
