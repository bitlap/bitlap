package org.bitlap.core.storage.carbon

import org.apache.hadoop.conf.Configuration
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.storage.StoreProvider
import org.bitlap.core.storage.store.MetricStore

class CarbonStoreProvider(val table: Table, val hadoopConf: Configuration) : StoreProvider {

    override fun getMetricStore(): MetricStore {
        return CarbonMetricStore(table, hadoopConf)
    }
}
