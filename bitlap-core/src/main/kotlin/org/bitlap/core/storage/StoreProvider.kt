/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.storage

/**
 * get store impl from provider
 */
interface StoreProvider {

    /**
     * get metric store
     */
    fun getMetricStore(): MetricStore

    /**
     * get metric with one low cardinality dimension store
     */
    fun getMetricDimStore(): MetricDimStore
}
