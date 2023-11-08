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

import org.bitlap.common.exception.BitlapException

import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import sttp.tapir.*
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.json.circe.jsonBody

/** HTTP Endpoints description
 */
trait HttpEndpoint:

  /** Custom Exception Schema
   */
  given Schema[BitlapException] =
    Schema[BitlapException](SchemaType.SProduct(Nil), Some(Schema.SName("BitlapException")))

  /** Custom exception codec
   */
  given exceptionCodec[A <: BitlapException]: JsonCodec[A] =
    implicitly[JsonCodec[io.circe.Json]].map(json =>
      json.as[A] match {
        case Left(_)      => throw new RuntimeException("MessageParsingError")
        case Right(value) => value
      }
    )(error => error.asJson)

  /** Custom exception serialization
   */
  given encodeException[A <: BitlapException]: Encoder[A] = (_: A) => Json.Null

  /** Custom exception deserialization
   */
  given decodeException[A <: BitlapException]: Decoder[A] =
    (c: HCursor) =>
      for {
        msg <- c.get[String]("msg")
      } yield BitlapException(msg).asInstanceOf[A]

  /** restful api description: api/sql/run
   *
   *  req: [[org.bitlap.server.http.SqlInput]]
   *
   *  resp: [[org.bitlap.server.http.SqlResult]]
   */
  lazy val runEndpoint: PublicEndpoint[SqlInput, BitlapException, SqlResult, Any] =
    endpoint.post
      .in("api" / "sql" / "run" / jsonBody[SqlInput])
      .errorOut(jsonBody[BitlapException])
      .out(jsonBody[SqlResult])

  /** restful api description: api/common/status
   *
   *  resp: String
   */
  lazy val statusEndpoint: PublicEndpoint[Unit, BitlapException, String, Any] =
    endpoint.get
      .in("api" / "common" / "status")
      .errorOut(jsonBody[BitlapException])
      .out(stringBody)
