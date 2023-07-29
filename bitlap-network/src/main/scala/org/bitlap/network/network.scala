/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap

import com.typesafe.scalalogging.LazyLogging

import io.grpc.*

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
package object network extends LazyLogging:

  type Identity[T] = T

  lazy val errorApplyFunc: Throwable => StatusException = (ex: Throwable) =>
    new StatusException(Status.fromThrowable(ex))

  extension [R <: AutoCloseable](r: R) def use[T](func: R => T): T = scala.util.Using.resource(r)(func)

  end extension

end network
