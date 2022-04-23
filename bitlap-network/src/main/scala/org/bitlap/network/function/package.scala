/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import io.grpc.Status

import java.io.IOException
import java.util.concurrent.TimeoutException

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/23
 */
package object function {

  val errorApplyFunc: Throwable => Status = (ex: Throwable) => {
    ex.printStackTrace()
    ex match {
      case _: TimeoutException | _: IOException => Status.UNAVAILABLE
      case _: NetworkException                  => Status.INTERNAL
    }
  }

  val statusApplyFunc: Status => Throwable = (st: Status) => {
    NetworkException(st.getCode.value(), st.getCode.toStatus.getDescription, st.asException())
  }

}
