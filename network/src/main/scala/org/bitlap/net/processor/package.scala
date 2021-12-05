package org.bitlap.net

import io.github.dreamylost.sofa.{ CustomRpcProcessor, Processable }
import org.apache.commons.lang.StringUtils
import org.bitlap.network.proto.driver.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.proto.driver.{ BStatus, BStatusCode }

import java.util.concurrent.Executor
import scala.jdk.CollectionConverters.MapHasAsScala

/**
 *
 * @author 梦境迷离
 * @since 2021/12/5
 * @version 1.0
 */
package object processor {

  def error(exception: Exception): BStatus = BStatus.newBuilder()
    .setStatusCode(BStatusCode.B_STATUS_CODE_ERROR_STATUS)
    .setErrorMessage(if (StringUtils.isBlank(exception.getMessage)) "" else exception.getMessage)
    .build()

  def success(): BStatus = BStatus.newBuilder().setStatusCode(BStatusCode.B_STATUS_CODE_SUCCESS_STATUS).build()

  // 没有发布到中央仓库
  def openSession(networkService: NetworkService): CustomRpcProcessor[BOpenSessionReq] = {
      Processable[NetworkService, BOpenSessionReq, BOpenSessionResp](networkService)(
        (service, _, req) => {
          val username = req.getUsername
          val password = req.getPassword
          val configurationMap = req.getConfigurationMap
          val ret = service.openSession(username, password, configurationMap.asScala.toMap)
          BOpenSessionResp.newBuilder().setSessionHandle(ret.toBSessionHandle()).setStatus(success()).build()
        },
        (_, _, exception) => {
          BOpenSessionResp.newBuilder().setStatus(error(exception)).build()
        }
      )
  }
}
