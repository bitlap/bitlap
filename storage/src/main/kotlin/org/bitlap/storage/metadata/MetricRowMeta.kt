package org.bitlap.storage.metadata

import cn.hutool.json.JSONObject

/**
 * Desc: Metadata for [org.bitlap.storage.metadata.MetricRow]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/2/22
 */
data class MetricRowMeta(
    val tm: Long,
    val metricKey: String,
    val entityKey: String,
    val entityUniqueCount: Long = 0,
    val entityCount: Long = 0,
    val metricCount: Double = 0.0
) {

    companion object {
        fun fromJson(jsonObj: JSONObject): MetricRowMeta =
            MetricRowMeta(
                jsonObj.getLong("tm"),
                jsonObj.getStr("metricKey"),
                jsonObj.getStr("entityKey"),
                jsonObj.getLong("entityUniqueCount"),
                jsonObj.getLong("entityCount"),
                jsonObj.getDouble("metricCount"),
            )
    }
}
