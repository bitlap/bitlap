/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm.plan

import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.FetchPlan
import org.bitlap.core.mdm.Fetcher

/**
 * abstract class for fetch plan
 */
abstract class AbsFetchPlan : FetchPlan {

    override val subPlans: List<FetchPlan> = mutableListOf()

    /**
     * fetch data with computed fetcher.
     */
    fun <R> withFetcher(context: FetchContext, fetch: (Fetcher) -> R): R {
        return fetch.invoke(context.findBestFetcher(this))
    }
}
