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
package org.bitlap.server.http.routes

import scala.collection.mutable.ListBuffer

import org.bitlap.common.exception.{ BitlapException, BitlapRuntimeException }

import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import sttp.tapir.{ AnyEndpoint, Endpoint, Schema, SchemaType }
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.ztapir.*
import zio.ZIO

trait BitlapRoute(name: String) {

  /** Custom Exception Schema
   */
  given Schema[BitlapException] =
    Schema[BitlapException](SchemaType.SProduct(Nil), Some(Schema.SName("BitlapException")))

  /** Custom exception codec
   */
  given exceptionCodec[A <: BitlapException]: JsonCodec[A] =
    implicitly[JsonCodec[io.circe.Json]].map(json =>
      json.as[A] match {
        case Left(_)      => throw new BitlapRuntimeException("MessageParsingError")
        case Right(value) => value
      }
    )(error => error.asJson)

  /** TODO Custom exception serialization
   */
  given encodeException[A <: BitlapException]: Encoder[A] = (_: A) => Json.Null

  /** TODO Custom exception deserialization
   */
  given decodeException[A <: BitlapException]: Decoder[A] =
    (c: HCursor) =>
      for {
        msg <- c.get[String]("msg")
      } yield BitlapException(msg).asInstanceOf[A]

  protected lazy val API: Endpoint[Unit, Unit, BitlapException, Unit, Any] =
    endpoint.in("api" / name).errorOut(customCodecJsonBody[BitlapException])
  protected lazy val endpoints: ListBuffer[(AnyEndpoint, ZServerEndpoint[Any, Any])] = ListBuffer.empty

  type BitlapEndpoint = Endpoint[Unit, Unit, BitlapException, Unit, Any]

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

  def getEndpoints: ListBuffer[(AnyEndpoint, ZServerEndpoint[Any, Any])] = endpoints
}
