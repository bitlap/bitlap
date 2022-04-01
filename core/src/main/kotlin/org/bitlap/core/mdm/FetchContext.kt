/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.mdm

import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.fetch.LocalFetcher
import org.bitlap.core.sql.QueryContext
import java.io.Serializable

/**
 * Fetch context wrapper
 */
class FetchContext : Serializable {
    lateinit var table: Table
    lateinit var queryContext: QueryContext
    lateinit var plan: FetchPlan

    /**
     * find best plan to be executed
     */
    fun findBestPlan(): FetchPlan {
        return plan
    }

    /**
     * find best data fetcher
     */
    fun findBestFetcher(plan: FetchPlan): Fetcher {
        return LocalFetcher(this)
    }
}
