/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import io.circe.Json
import io.circe.parser._
import zhttp.http.HttpData
import zio.ZIO

import java.nio.charset.Charset

/** Mail: k.chen@nio.com Created by IceMimosa Date: 2022/11/17
 */
package object http {

  implicit class implicits(body: HttpData) {
    def toJson: ZIO[Any, Throwable, Json] =
      body.toByteBuf
        .map(buf => buf.toString(Charset.defaultCharset()))
        .map(str => parse(str).getOrElse(Json.Null))
  }
}
