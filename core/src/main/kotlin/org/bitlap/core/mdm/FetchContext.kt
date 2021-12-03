package org.bitlap.core.mdm

import org.bitlap.common.BitlapConf
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.fetch.LocalFetcher
import java.io.Serializable

/**
 * Fetch context wrapper
 */
class FetchContext : Serializable {
    lateinit var runtimeConf: BitlapConf
    lateinit var table: Table
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
