/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm

import org.bitlap.core.mdm.model.RowIterator

/**
 * Fetch plan
 */
interface FetchPlan {

    /**
     * sub plans
     */
    val subPlans: List<FetchPlan>

    /**
     * if optimized for current plan
     */
    var optimized: Boolean

    /**
     * execute current plan
     */
    fun execute(context: FetchContext): RowIterator

    /**
     * execute plan graph
     */
    fun explain(depth: Int = 0): String
}
