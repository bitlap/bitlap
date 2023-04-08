/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.http

import io.circe._
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe.jsonBody
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.json.circe._
import org.bitlap.network.NetworkException
import org.bitlap.network.NetworkException.SQLExecutedException

/** @author
 *    梦境迷离
 *  @version 1.0,2023/4/8
 */
trait HttpEndpoint {

  implicit val exceptionSchema: Schema[NetworkException] =
    Schema[NetworkException](SchemaType.SProduct(Nil), Some(Schema.SName("NetworkException")))

  implicit def exceptionCodec[A <: NetworkException]: JsonCodec[A] =
    implicitly[JsonCodec[io.circe.Json]].map(json =>
      json.as[A] match {
        case Left(_)      => throw new RuntimeException("MessageParsingError")
        case Right(value) => value
      }
    )(error => error.asJson)

  implicit def encodeException[A <: NetworkException]: Encoder[A] = (_: A) => Json.Null

  implicit def decodeException[A <: NetworkException]: Decoder[A] =
    (c: HCursor) =>
      for {
        msg <- c.get[String]("msg")
      } yield SQLExecutedException(msg = msg).asInstanceOf[A]

  lazy val runEndpoint: PublicEndpoint[SqlInput, NetworkException, SqlResult, Any] =
    endpoint.post
      .in("api" / "sql" / "run" / jsonBody[SqlInput])
      .errorOut(jsonBody[NetworkException])
      .out(jsonBody[SqlResult])

  lazy val statusEndpoint: PublicEndpoint[Unit, NetworkException, String, Any] =
    endpoint.get
      .in("api" / "common" / "status")
      .errorOut(jsonBody[NetworkException])
      .out(stringBody)

}
