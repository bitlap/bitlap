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

  lazy val errorApplyFunc: Throwable => Status = (ex: Throwable) => {
    logger.error("Grpc server exception", ex)
    Status.fromThrowable(ex)
  }

  lazy val statusApplyFunc: Status => Throwable = (st: Status) => {
    logger.error("Grpc client exception: {}", st)
    st.asException()
  }
}
