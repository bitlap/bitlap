/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.http
import org.bitlap.server.BitlapServerProvider
import zhttp.http.Method
import zhttp.http.Response
import zhttp.http.Request
import zhttp.service.Server
import zhttp.http._
import zio._
import zhttp.service.server.ServerChannelFactory
import zhttp.service.EventLoopGroup

import scala.util.Try

final class HttpServerProvider(val port: Int) extends BitlapServerProvider {

  private val app = Http.collectZIO[Request] {
    case Method.GET -> !! / "random" => random.nextString(10).map(Response.text)
    case Method.GET -> !! / "utc"    => clock.currentDateTime.map(s => Response.text(s.toString))
  }

  private val server = Server.port(port) ++ Server.paranoidLeakDetection ++ Server.app(app)

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val nThreads: Int = args.headOption.flatMap(x => Try(x.toInt).toOption).getOrElse(0)
    server.make
      .use(_ =>
        console.putStrLn(s"$serverType: Server started on port $port")
          *> ZIO.never
      )
      .provideCustomLayer(ServerChannelFactory.auto ++ EventLoopGroup.auto(nThreads))
      .exitCode
  }

  override def serverType: String = "HTTP"
}
