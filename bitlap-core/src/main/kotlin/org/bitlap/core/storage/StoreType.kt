/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.storage

import org.apache.hadoop.conf.Configuration
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.storage.carbon.CarbonStoreProvider
import org.bitlap.core.storage.store.DefaultStoreProvider

/**
 * storage type
 */
enum class StoreType {

    /**
     * default implementation
     */
    NONE {
        override fun getProvider(table: Table, hadoopConf: Configuration): StoreProvider {
            return DefaultStoreProvider()
        }
    },

    /**
     * implementation for apache carbon data
     */
    CARBON {
        override fun getProvider(table: Table, hadoopConf: Configuration): StoreProvider {
            return CarbonStoreProvider(table, hadoopConf)
        }
    };

    abstract fun getProvider(table: Table, hadoopConf: Configuration): StoreProvider
}
