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

import scala.collection.mutable.ListBuffer

import org.bitlap.common.exception._
import org.bitlap.server.http.Response

import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import sttp.tapir.{ Schema, SchemaType }
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.json.circe.*
import sttp.tapir.ztapir.*
import zio.ZIO

/** Provide a custom decoder, encoder, schema for [[org.bitlap.common.exception.BitlapThrowable]]
 */
trait BitlapCodec {

  // Custom Exception Schema
  given Schema[BitlapThrowable] =
    Schema[BitlapThrowable](SchemaType.SProduct(Nil), Some(Schema.SName("BitlapThrowable")))

  // Custom exception serialization
  given encodeException[A <: BitlapThrowable]: Encoder[A] = (e: A) => Response.fail(e.asInstanceOf[Throwable]).asJson

  // Custom exception deserialization
  given decodeException[A <: BitlapThrowable]: Decoder[A] = (c: HCursor) =>
    for {
      msg <- c.get[String]("msg")
    } yield BitlapException(msg).asInstanceOf[A]

  // Custom exception codec
  given exceptionCodec[A <: BitlapThrowable]: JsonCodec[A] =
    implicitly[JsonCodec[io.circe.Json]].map(json =>
      json.as[A] match {
        case Left(_)      => throw new IllegalArgumentException("MessageParsingError")
        case Right(value) => value
      }
    )(_.asJson)

}
