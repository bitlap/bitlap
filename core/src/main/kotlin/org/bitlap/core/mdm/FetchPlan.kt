package org.bitlap.core.mdm

import org.bitlap.common.BitlapIterator

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
    fun execute(context: FetchContext): BitlapIterator<Array<*>>
}
