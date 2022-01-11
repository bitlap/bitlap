package org.bitlap.core.storage

import org.bitlap.core.storage.store.MetricStore

/**
 * get store impl from provider
 */
interface StoreProvider {

    /**
     * get metric store
     */
    fun getMetricStore(): MetricStore
}
