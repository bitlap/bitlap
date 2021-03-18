package org.bitlap.core.model.query

/**
 * Desc: Base query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/17
 */
data class Query(
    /**
     * @required datasource name
     */
    val datasource: String,

    /**
     * @required time
     */
    val time: Time,

    /**
     * @required entity name
     */
    val entity: String,

    /**
     * metric name
     */
    val metric: String
)
