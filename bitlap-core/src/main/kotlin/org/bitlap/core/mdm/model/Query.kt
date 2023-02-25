/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm.model

import org.bitlap.core.Constants

/**
 * Desc: Base query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/17
 */
data class Query(

    /**
     * @nullable database name
     */
    val database: String = Constants.DEFAULT_DATABASE,

    /**
     * @required table name
     */
    val table: String,

    /**
     * @required time
     */
    val time: QueryTime,

    /**
     * @required entity name
     */
    val entity: String,

    /**
     * @required query metrics
     */
    val metrics: List<QueryMetric>,

    /**
     * query dimensions
     */
    val dimensions: List<String> = emptyList(),

    /**
     * query filters
     */
    val filters: List<QueryFilter> = emptyList(),

    /**
     * query limit
     */
    val limit: Int = 10000,
) {

    fun hasDimensions(): Boolean {
        return dimensions.isNotEmpty() || filters.isNotEmpty()
    }
}
