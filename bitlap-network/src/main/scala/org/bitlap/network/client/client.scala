/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import io.grpc.Status
import org.bitlap.network.driver.proto.{ BStatus, BStatusCode }
import zio.ZIO

import scala.reflect.{ classTag, ClassTag }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
package object client {

  type Identity[X] = X

  lazy val runtime = zio.Runtime.global

  def blocking[T: ClassTag, Z: ClassTag](action: => Z)(t: T => T): T =
    classTag[T].runtimeClass match {
      case clazz if clazz.isAssignableFrom(classOf[ZIO[Any, Throwable, T]]) =>
        t.apply(runtime.unsafeRun(action.asInstanceOf[ZIO[Any, Throwable, T]]))
      case _ => throw exception(Status.INTERNAL)
    }

  @inline def exception(st: Status) = NetworkException(st.getCode.value(), st.getCode.toStatus.getDescription)

  /**
   * Used to verify whether the RPC result is correct.
   */
  @inline def verifySuccess[T](status: BStatus, t: T): T =
    if (t == null || status.statusCode != BStatusCode.B_STATUS_CODE_SUCCESS_STATUS) {
      throw new Exception(status.errorMessage)
    } else {
      t
    }
}
