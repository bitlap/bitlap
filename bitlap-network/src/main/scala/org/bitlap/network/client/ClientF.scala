/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.client
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
trait ClientF[F[_]] {

  def openSession(request: BOpenSessionReq): F[BOpenSessionResp]

  def closeSession(request: BCloseSessionReq): F[BCloseSessionResp]

  def executeStatement(request: BExecuteStatementReq): F[BExecuteStatementResp]
}

object ClientF {

  def newSyncClient(uri: String, port: Int) = new SyncClient(uri, port)

  class SyncClient(uri: String, port: Int) extends ClientF[Identity] {

    private lazy val delegate = ClientEF.newZIOClient(uri, port)

    override def openSession(request: BOpenSessionReq): Identity[BOpenSessionResp] =
      blocking {
        delegate.openSession(request)
      } { t: BOpenSessionResp =>
        verifySuccess(t.getStatus, t)
      }

    override def closeSession(request: BCloseSessionReq): Identity[BCloseSessionResp] =
      blocking {
        delegate.closeSession(request)
      } { t: BCloseSessionResp =>
        verifySuccess(t.getStatus, t)
      }

    override def executeStatement(request: BExecuteStatementReq): Identity[BExecuteStatementResp] =
      blocking {
        delegate.executeStatement(request)
      } { t: BExecuteStatementResp =>
        verifySuccess(t.getStatus, t)
      }
  }

}
