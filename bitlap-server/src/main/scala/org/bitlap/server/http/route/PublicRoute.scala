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
package org.bitlap.server.http.route

import scala.collection.mutable.ListBuffer

import org.bitlap.common.exception.*
import org.bitlap.server.http._

import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import sttp.model.StatusCode
import sttp.tapir.{ AnyEndpoint, Endpoint, Schema, SchemaType }
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.json.circe.*
import sttp.tapir.server.interceptor.CustomiseInterceptors
import sttp.tapir.ztapir.*
import zio.ZIO

/** Abstract routes for bitlap public endpoints.
 */
trait PublicRoute(name: String) extends CustomExceptionCodec {

  // common api
  protected lazy val API: BitlapEndpoint =
    endpoint.in("api" / name).errorOut(customCodecJsonBody[BitlapThrowable])

  type BitlapEndpoint = Endpoint[Unit, Unit, BitlapThrowable, Unit, Any]

  protected lazy val endpoints: ListBuffer[(AnyEndpoint, ZServerEndpoint[Any, Any])] = ListBuffer.empty

  protected def get[A, I, E, O, R, RR](
    point: BitlapEndpoint => Endpoint[A, I, E, O, R]
  )(
    logic: I => ZIO[RR, E, O]
  )(using aIsUnit: A =:= Unit
  ): ZServerEndpoint[RR, R] = {
    val ed     = point(API.get)
    val zLogic = ed.zServerLogic[RR](logic)(using aIsUnit)
    endpoints += ed -> zLogic.asInstanceOf[ZServerEndpoint[Any, Any]]
    zLogic
  }

  protected def post[A, I, E, O, R, RR](
    point: BitlapEndpoint => Endpoint[A, I, E, O, R]
  )(
    logic: I => ZIO[RR, E, O]
  )(using aIsUnit: A =:= Unit
  ): ZServerEndpoint[RR, R] = {
    val ed     = point(API.post)
    val zLogic = ed.zServerLogic[RR](logic)(using aIsUnit)
    endpoints += ed -> zLogic.asInstanceOf[ZServerEndpoint[Any, Any]]
    zLogic
  }

  def getEndpoints: List[(AnyEndpoint, ZServerEndpoint[Any, Any])] = endpoints.toList

  // make zio response
  extension [A](zio: ZIO[Any, Throwable, A]) {

    // make response
    def response: ZIO[Any, BitlapThrowable, Response[A]] = zio.mapBoth(
      {
        case ex: BitlapHttpException           => ex
        case ex: BitlapAuthenticationException => ex
        case ex: BitlapThrowable => BitlapExceptions.httpException(StatusCode.InternalServerError.code, ex.errorKey)
        case ex                  => BitlapExceptions.unknownException(ex)
      },
      {
        case rr: Response[_] => rr.asInstanceOf[Response[A]]
        case rr: A           => Response.ok(rr)
      }
    )
  }

}
