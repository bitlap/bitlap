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

  /** TODO grpc client 获取不到cause，错误内容放到description中
   */
  lazy val errorApplyFunc: Throwable => Status = (ex: Throwable) =>
    Status.INTERNAL.withDescription(ex.getLocalizedMessage)

  lazy val statusApplyFunc: Status => Throwable = (st: Status) => st.asException()
}
