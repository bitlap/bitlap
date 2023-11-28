/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server.http

import org.bitlap.common.exception.BitlapHttpException

case class Response[T](
  data: Option[T],
  success: Boolean,
  code: Int,
  error: Option[String])

object Response {

  val SUCCESS: Int = 0
  val FAIL: Int    = -1

  def ok[T](data: T): Response[T] =
    Response(Option(data), true, SUCCESS, None)

  def fail(e: Throwable): Response[String] =
    e match {
      // TODO (make code message)
      case ex: BitlapHttpException => Response[String](None, false, ex.code, Option(e.getMessage))
      case ex                      => Response[String](None, false, FAIL, Option(ex.getMessage))
    }

  def fail(code: Int, msg: String): Response[String] =
    Response[String](None, false, code, Option(msg))
}
