/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.http

/** Desc: response 对象
 */
case class Res(data: Any, success: Boolean, errorCode: Int = -1, errorMsg: String = "")

object Res {
  def ok(data: Any): Res = Res(data, success = true)

  def fail(errorCode: Int, errorMsg: String): Res = Res(null, success = false)
}
