/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import io.grpc.Status
import org.bitlap.network.NetworkException._

import java.io.IOException
import java.util.concurrent.TimeoutException

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
package object network {

  type Identity[X] = X

  lazy val errorApplyFunc: Throwable => Status = (ex: Throwable) => {
    ex.printStackTrace()
    ex match {
      case _: TimeoutException | _: IOException => Status.UNAVAILABLE
      case _: RpcException                      => Status.INTERNAL
      case _: LeaderServerNotFoundException     => Status.ABORTED
      case _: Exception                         => Status.UNKNOWN
    }
  }

  lazy val statusApplyFunc: Status => Throwable = (st: Status) =>
    RpcException(st.getCode.value(), st.getCode.toStatus.getDescription, Option(st.asException()))
}
