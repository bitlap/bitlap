/* Copyright (c) 2023 bitlap.org */
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
     * execute current plan
     */
    fun execute(context: FetchContext): RowIterator
}
