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

import org.bitlap.common.exception._

import io.circe.Encoder._
import sttp.model.{ Header, StatusCode }
import sttp.monad.MonadError
import sttp.tapir._
import sttp.tapir.{ server, Codec, DecodeResult, EndpointIO, EndpointOutput }
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.interceptor.DecodeFailureContext
import sttp.tapir.server.interceptor.decodefailure.DefaultDecodeFailureHandler
import sttp.tapir.server.interceptor.decodefailure.DefaultDecodeFailureHandler.FailureMessages
import sttp.tapir.server.interceptor.exception._
import sttp.tapir.server.model.ValuedEndpointOutput
import zio.Task

/** Convert all errors to [[org.bitlap.common.exception.BitlapHttpException]]
 */
trait BitlapExceptionHandler {
  self: BitlapCodec =>

  import self.given

  def decodeFailureHandler: DefaultDecodeFailureHandler[Task] =
    new DefaultDecodeFailureHandler[Task](
      DefaultDecodeFailureHandler.respond,
      FailureMessages.failureMessage,
      defaultFailureDecode
    )
  end decodeFailureHandler

  private def defaultFailureDecode(c: StatusCode, hs: List[Header], m: String): ValuedEndpointOutput[_] =
    server.model.ValuedEndpointOutput(
      statusCode.and(headers).and(jsonBody[BitlapThrowable]),
      (
        c,
        hs,
        BitlapHttpException(
          m,
          code = StatusCode.PreconditionFailed.code
        )
      )
    )

  end defaultFailureDecode

  def defaultFailureResponse(m: String): ValuedEndpointOutput[_] =
    ValuedEndpointOutput(jsonBody[BitlapThrowable], BitlapHttpException(m, code = StatusCode.InternalServerError.code))

  def exceptionHandler: ExceptionHandler[Task] = ExceptionHandler.pure[Task](ctx =>
    Some(
      ctx.e match
        case ex: BitlapAuthenticationException =>
          ValuedEndpointOutput(
            jsonBody[BitlapThrowable],
            BitlapHttpException(ex.errorKey, code = StatusCode.Unauthorized.code)
          )
        case ex: BitlapHttpException =>
          ValuedEndpointOutput(jsonBody[BitlapThrowable], BitlapHttpException(ex.errorKey, code = ex.code))
        case ex => defaultFailureResponse(ex.getMessage)
    )
  )

}
