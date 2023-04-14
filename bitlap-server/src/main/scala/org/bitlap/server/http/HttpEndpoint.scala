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

/** HTTP API 定义描述
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2023/4/8
 */
trait HttpEndpoint {

  /** 自定义异常Schema
   */
  implicit val exceptionSchema: Schema[NetworkException] =
    Schema[NetworkException](SchemaType.SProduct(Nil), Some(Schema.SName("NetworkException")))

  /** 自定义异常编解码器
   */
  implicit def exceptionCodec[A <: NetworkException]: JsonCodec[A] =
    implicitly[JsonCodec[io.circe.Json]].map(json =>
      json.as[A] match {
        case Left(_)      => throw new RuntimeException("MessageParsingError")
        case Right(value) => value
      }
    )(error => error.asJson)

  /** 自定义异常序列化
   */
  implicit def encodeException[A <: NetworkException]: Encoder[A] = (_: A) => Json.Null

  /** 自定义异常反序列化
   */
  implicit def decodeException[A <: NetworkException]: Decoder[A] =
    (c: HCursor) =>
      for {
        msg <- c.get[String]("msg")
      } yield SQLExecutedException(msg = msg).asInstanceOf[A]

  /** restful api描述: api/sql/run
   *
   *  req: [[org.bitlap.server.http.SqlInput]]
   *
   *  resp: [[org.bitlap.server.http.SqlResult]]
   */
  lazy val runEndpoint: PublicEndpoint[SqlInput, NetworkException, SqlResult, Any] =
    endpoint.post
      .in("api" / "sql" / "run" / jsonBody[SqlInput])
      .errorOut(jsonBody[NetworkException])
      .out(jsonBody[SqlResult])

  /** restful api描述: api/common/status
   *
   *  resp: 字符串
   */
  lazy val statusEndpoint: PublicEndpoint[Unit, NetworkException, String, Any] =
    endpoint.get
      .in("api" / "common" / "status")
      .errorOut(jsonBody[NetworkException])
      .out(stringBody)

}
