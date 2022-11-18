/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import io.circe.Json
import io.circe.parser._
import zhttp.http.HttpData
import zio.ZIO

import java.nio.charset.Charset

package object http {

  implicit final class implicits(val body: HttpData) extends AnyVal {
    def toJson: ZIO[Any, Throwable, Json] =
      body.toByteBuf.map { buf =>
        val r = buf.toString(Charset.defaultCharset())
        buf.release()
        r
      }
        .map(str => parse(str).getOrElse(Json.Null))
  }
}
