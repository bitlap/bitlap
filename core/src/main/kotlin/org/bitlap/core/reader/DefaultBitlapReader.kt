package org.bitlap.core.reader

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.BitlapReader
import org.bitlap.core.DataSourceManager
import org.bitlap.core.model.query.Query

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/17
 */
class DefaultBitlapReader : BitlapReader {

    override fun read(query: Query) {
        val dsStore = DataSourceManager.getDataSourceStore(PreConditions.checkNotBlank(query.datasource))
        val metricStore = dsStore.getMetricStore()
        val metas = metricStore.queryMeta(query.time.start, query.metric, query.entity)

        println(metas)
    }

    override fun close() {
    }
}
