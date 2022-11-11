/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import io.grpc.Status
import org.bitlap.network.NetworkException._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
package object network {

  type Identity[X] = X

  lazy val errorApplyFunc: Throwable => Status = (ex: Throwable) => {
    ex.printStackTrace()
    ex match {
      case e: RpcException =>
        Status.INTERNAL.withDescription(e.getLocalizedMessage).withCause(e.cause.orNull)
      case e: LeaderServerNotFoundException =>
        Status.ABORTED.withDescription(e.getLocalizedMessage).withCause(e.cause.orNull)
      case e: SQLExecuteException =>
        Status.INVALID_ARGUMENT.withDescription(e.getLocalizedMessage).withCause(e.cause.orNull)
      case e: Exception =>
        Status.UNKNOWN.withDescription(e.getLocalizedMessage).withCause(e.getCause)
    }
  }

  lazy val statusApplyFunc: Status => Throwable = (st: Status) =>
    st match {
      case Status.INTERNAL =>
        RpcException(st.getCode.value(), st.getCode.toStatus.getDescription, Option(st.asException()))
      case Status.ABORTED =>
        LeaderServerNotFoundException(st.getCode.toStatus.getDescription, Option(st.asException()))
      case Status.INVALID_ARGUMENT =>
        SQLExecuteException(st.getCode.toStatus.getDescription, Option(st.asException()))
      case Status.UNKNOWN =>
        new Exception(st.getCode.toStatus.getDescription, st.asException())
    }

}
