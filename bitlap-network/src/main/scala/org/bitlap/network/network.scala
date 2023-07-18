/* Copyright (c) 2023 bitlap.org */
package org.bitlap

import com.typesafe.scalalogging.LazyLogging

import io.grpc.*

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
package object network extends LazyLogging:

  type Identity[T] = T

  /** TODO grpc client 获取不到cause，错误内容放到description中
   */
  lazy val errorApplyFunc: Throwable => StatusException = (ex: Throwable) =>
    new StatusException(Status.fromThrowable(ex))

  extension [R <: AutoCloseable](r: R)

    def use[T](func: R => T): T = {
      try {
        func(r)
      } finally {
        try {
          r.close()
        } catch {
          case _: Throwable =>
        }
      }
    }
  end extension

end network
