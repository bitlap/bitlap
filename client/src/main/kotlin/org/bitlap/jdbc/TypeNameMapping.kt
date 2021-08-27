package org.bitlap.jdbc

import org.bitlap.network.proto.driver.BTypeId

/**
 *
 * @author li.guobin@immomo.com
 * @version 1.0,2021/8/27
 */
object TypeNameMapping {

    val typeNames by lazy {
        mapOf(
            Pair(BTypeId.B_TYPE_ID_STRING_TYPE, "STRING"),
            Pair(BTypeId.B_TYPE_ID_INT_TYPE, "INT"),
            Pair(BTypeId.B_TYPE_ID_DOUBLE_TYPE, "DOUBLE")
        )
    }
}
