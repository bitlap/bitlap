/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import io.grpc.Status
import org.bitlap.network.NetworkException.RpcException
import zio.{ IO, ZIO }

import java.io.IOException
import java.util.concurrent.TimeoutException
import scala.concurrent.Future

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
package object network {

  type Identity[X] = X

  lazy val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.global

  lazy val errorApplyFunc: Throwable => Status = (ex: Throwable) => {
    ex.printStackTrace()
    ex match {
      case _: TimeoutException | _: IOException => Status.UNAVAILABLE
      case _: RpcException                      => Status.INTERNAL
    }
  }

  lazy val statusApplyFunc: Status => Throwable = (st: Status) =>
    RpcException(st.getCode.value(), Option(st.getCode.toStatus.getDescription), Option(st.asException()))

  def blocking[T, Z <: ZIO[_, _, _]](action: => Z): T = runtime.unsafeRun(action.asInstanceOf[ZIO[Any, Throwable, T]])

  def zioFrom[T](action: => T): ZIO[Any, Status, T] =
    IO.effect(action).mapError { ex => ex.printStackTrace(); Status.fromThrowable(ex) }

  def zioFromFuture[T, R, F[_] <: Future[_]](action: => F[T])(t: T => R): ZIO[Any, Status, R] =
    IO.fromFuture[T](make => action.asInstanceOf[Future[T]])
      .map(hd => t(hd))
      .mapError(errorApplyFunc)
}
