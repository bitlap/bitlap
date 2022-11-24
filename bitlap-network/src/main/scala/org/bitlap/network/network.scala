/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import io.grpc.Status
import org.bitlap.network.NetworkException._
import org.bitlap.common.exception.SQLExecutedException

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
package object network {

  type Identity[T] = T

  lazy val errorApplyFunc: Throwable => Status = (ex: Throwable) => {
    ex.printStackTrace()
    ex match {
      case e: RpcException =>
        Status.INTERNAL.withDescription(e.getLocalizedMessage).withCause(e.cause.orNull)
      case e: LeaderNotFoundException =>
        Status.ABORTED.withDescription(e.getLocalizedMessage).withCause(e.cause.orNull)
      case e: SQLExecutedException =>
        Status.INVALID_ARGUMENT.withDescription(e.getLocalizedMessage).withCause(e.getCause)
      case e =>
        Status.UNKNOWN.withDescription(e.getLocalizedMessage).withCause(e.getCause)
    }
  }

  lazy val statusApplyFunc: Status => Throwable = (st: Status) => {
    st.asException().printStackTrace()
    st.getCode match {
      case c if c.value() == Status.INTERNAL.getCode.value() =>
        RpcException(st.getCode.value(), st.getCode.toStatus.getDescription, Option(st.asException()))
      case c if c.value() == Status.ABORTED.getCode.value() =>
        LeaderNotFoundException(st.getCode.toStatus.getDescription, Option(st.asException()))
      case c if c.value() == Status.INVALID_ARGUMENT.getCode.value() =>
        new SQLExecutedException(Option(st.getCode.toStatus.getDescription).getOrElse(""), st.asException())
      case c if c.value() == Status.UNKNOWN.getCode.value() =>
        new Exception(st.getCode.toStatus.getDescription, st.asException())
      case _ => new Exception(st.getCode.toStatus.getDescription, st.asException())
    }
  }
}
