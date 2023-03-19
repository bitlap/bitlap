/* Copyright (c) 2023 bitlap.org */
package org.bitlap

import com.typesafe.scalalogging.LazyLogging
import io.grpc.Status

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
package object network extends LazyLogging {

  type Identity[T] = T

  lazy val errorApplyFunc: Throwable => Status = (ex: Throwable) =>
    Status.INTERNAL.withCause(ex).withDescription("Grpc server exception")

  // TODO grpc cause 丢失
  lazy val statusApplyFunc: Status => Throwable = (st: Status) =>
    st.withDescription("Grpc client exception").asException()
}
