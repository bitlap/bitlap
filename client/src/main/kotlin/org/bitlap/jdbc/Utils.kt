package org.bitlap.jdbc

import org.bitlap.network.proto.driver.BTypeId

/**
 *
 * @author 梦境迷离
 * @since 2021/8/28
 * @version 1.0
 */
object Utils {

    /**
     * Major version number of this driver.
     */
    const val MAJOR_VERSION = 0

    /**
     * Minor version number of this driver.
     */
    const val MINOR_VERSION = 0

    /**
     * Is this driver JDBC compliant?
     */
    const val JDBC_COMPLIANT = false

    /**
     * The required prefix for the connection url
     */
    const val URL_PREFIX = "jdbc:bitlap://"

    /**
     * If host is provided, without a port
     */
    const val DEFAULT_PORT = "10000"

    /**
     * Property key for the database name
     */
    const val DBNAME_PROPERTY_KEY = "DBNAME"

    /**
     * Property key for the bitlap Server host
     */
    const val HOST_PROPERTY_KEY = "HOST"

    /**
     * Property key for the bitlap Server port
     */
    const val PORT_PROPERTY_KEY = "PORT"

    val typeNames by lazy {
        mapOf(
            Pair(BTypeId.B_TYPE_ID_STRING_TYPE, "STRING"),
            Pair(BTypeId.B_TYPE_ID_INT_TYPE, "INT"),
            Pair(BTypeId.B_TYPE_ID_DOUBLE_TYPE, "DOUBLE"),
            Pair(BTypeId.B_TYPE_ID_SHORT_TYPE, "SHORT"),
            Pair(BTypeId.B_TYPE_ID_BOOLEAN_TYPE, "BOOLEAN"),
            Pair(BTypeId.B_TYPE_ID_LONG_TYPE, "LONG"),
            Pair(BTypeId.B_TYPE_ID_TIMESTAMP_TYPE, "TIMESTAMP")
        )
    }
}
