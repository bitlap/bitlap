/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import io.grpc.Status
import org.bitlap.network.rpc.runtime
import zio.{ IO, ZIO }

import scala.concurrent.Future

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/22
 */
package object dsl {

  def blocking[T, Z <: ZIO[_, _, _]](action: => Z): T = runtime.unsafeRun(action.asInstanceOf[ZIO[Any, Throwable, T]])

  def zioFrom[T](action: => T): ZIO[Any, Status, T] =
    IO.effect(action).mapError { ex => ex.printStackTrace(); Status.fromThrowable(ex) }

  def zioFromFuture[T, R, F[_] <: Future[_]](action: => F[T])(t: T => R): ZIO[Any, Status, R] =
    IO.fromFuture[T](make => action.asInstanceOf[Future[T]])
      .map(hd => t(hd))
      .mapError { f => f.printStackTrace(); Status.INTERNAL }
}
