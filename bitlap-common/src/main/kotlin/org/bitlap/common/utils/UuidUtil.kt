/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common.utils

import java.util.UUID

/**
 *
 * @author 梦境迷离
 * @version 1.0,2022/10/31
 */
object UuidUtil {

    @JvmStatic
    fun uuid(): String {
        return UUID.randomUUID().toString().replace("-", "")
    }
}
