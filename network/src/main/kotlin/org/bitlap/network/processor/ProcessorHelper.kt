package org.bitlap.network.processor

import org.bitlap.network.proto.driver.BStatus
import org.bitlap.network.proto.driver.BStatusCode

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface ProcessorHelper {

    fun success(): BStatus = BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_SUCCESS_STATUS).build()

    fun error(): BStatus = BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_ERROR_STATUS).build()

    fun executing(): BStatus =
        BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_STILL_EXECUTING_STATUS).build()

    fun invalidHandle(): BStatus =
        BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_INVALID_HANDLE_STATUS).build()
}
