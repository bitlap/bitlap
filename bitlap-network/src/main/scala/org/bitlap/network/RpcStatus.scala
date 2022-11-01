/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.driver.proto._

/** bitlap RPC错误码
 *  @author
 *    梦境迷离
 *  @since 2021/12/5
 *  @version 1.0
 */
trait RpcStatus {

  def successOpt(): Option[BStatus] = Some(success())

  def errorOpt(exception: Exception): Option[BStatus] = Some(error(exception))

  def success(): BStatus = BStatus(BStatusCode.B_STATUS_CODE_SUCCESS_STATUS)

  def error(msg: String = ""): BStatus = BStatus(BStatusCode.B_STATUS_CODE_ERROR_STATUS, msg)

  def error(exception: Exception): BStatus = BStatus(BStatusCode.B_STATUS_CODE_ERROR_STATUS, exception.getMessage)

  def executing(): BStatus = BStatus(BStatusCode.B_STATUS_CODE_STILL_EXECUTING_STATUS)

  def invalidHandle(): BStatus = BStatus(BStatusCode.B_STATUS_CODE_INVALID_HANDLE_STATUS)
}
