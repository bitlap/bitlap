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

  implicit final class RpcFutureOps(val rpc: RpcFuture) extends AnyVal {
    def transform[A, B](fx: RpcFuture => Future[A])(f: A => B): ZIO[Any, Status, B] =
      IO.fromFuture[A](make => fx(rpc))
        .map(hd => f(hd))
        .mapError(errorApplyFunc)
  }

  implicit final class RpcIdentityOps(val rpc: RpcIdentity) extends AnyVal {
    def zio[T](action: => T): ZIO[Any, Status, T] =
      IO.effect(action).mapError { ex => ex.printStackTrace(); Status.fromThrowable(ex) }
  }
}
