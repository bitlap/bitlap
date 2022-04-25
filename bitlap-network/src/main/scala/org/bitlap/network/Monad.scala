/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import io.grpc.Status
import org.bitlap.network.dsl.zioFromFuture
import org.bitlap.network.function.errorApplyFunc
import zio.ZIO

import scala.concurrent.Future

/**
 * Monad for backend, this is because not all subclasses support it.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/25
 */
object Monad {

  def mapBoth[R, A, B](r: R)(fx: R => ZIO[Any, Throwable, A])(f: A => B): ZIO[Any, Status, B] =
    fx(r).map(f).mapError(errorApplyFunc)

  def flatMapBoth[R, A, B](r: R)(fx: R => ZIO[Any, Throwable, A])(
    f: A => ZIO[Any, Throwable, B]
  ): ZIO[Any, Status, B] =
    fx(r).flatMap(f).mapError(errorApplyFunc)

  def transform[R, A, B](r: R)(fx: R => Future[A])(f: A => B): ZIO[Any, Status, B] = zioFromFuture(fx(r))(f)

}
