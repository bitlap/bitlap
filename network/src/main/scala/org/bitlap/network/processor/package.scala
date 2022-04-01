/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.apache.commons.lang.StringUtils
import org.bitlap.network.proto.driver.{ BStatus, BStatusCode }

/**
 * @author 梦境迷离
 * @since 2021/12/5
 * @version 1.0
 */
package object processor {

  def success(): BStatus = BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_SUCCESS_STATUS).build()

  def error(msg: String = ""): BStatus = BStatus
    .newBuilder()
    .setStatusCode(BStatusCode.B_STATUS_CODE_ERROR_STATUS)
    .setErrorMessage(msg)
    .build()

  def error(exception: Exception): BStatus = BStatus
    .newBuilder()
    .setStatusCode(BStatusCode.B_STATUS_CODE_ERROR_STATUS)
    .setErrorMessage(if (StringUtils.isBlank(exception.getMessage)) "" else exception.getMessage)
    .build()

  def executing(): BStatus =
    BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_STILL_EXECUTING_STATUS).build()

  def invalidHandle(): BStatus =
    BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_INVALID_HANDLE_STATUS).build()
}
