package org.bitlap.core.storage.carbon

import org.apache.hadoop.conf.Configuration
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.storage.MetricDimStore
import org.bitlap.core.storage.MetricStore
import org.bitlap.core.storage.StoreProvider

class CarbonStoreProvider(val table: Table, val hadoopConf: Configuration) : StoreProvider {

    override fun getMetricStore(): MetricStore {
        return CarbonMetricStore(table, hadoopConf)
    }

    override fun getMetricDimStore(): MetricDimStore {
        return CarbonMetricDimStore(table, hadoopConf)
    }
}
