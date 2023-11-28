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

import org.bitlap.server.http.routes.{ ResourceRoute, SqlRoute }

import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import zio.*
import zio.http.HttpApp

object HttpRoutes {

  val live: ZLayer[ResourceRoute & SqlRoute, Nothing, HttpRoutes] =
    ZLayer.fromFunction((commonRoute: ResourceRoute, sqlRoute: SqlRoute) => HttpRoutes(commonRoute, sqlRoute))
}

final class HttpRoutes(commonRoute: ResourceRoute, sqlRoute: SqlRoute) {

  private val swaggerEndpoints: List[ServerEndpoint[Any, Task]] = SwaggerInterpreter().fromServerEndpoints[Task](
    endpoints = (sqlRoute.getEndpoints.map(_._2) ++ commonRoute.getEndpoints.map(_._2)).toList,
    title = "Bitlap API",
    version = "1.0"
  )

  def getHttpApp: HttpApp[Any] = ZioHttpInterpreter().toHttp[Any](
    (sqlRoute.getEndpoints.map(_._2) ++ commonRoute.getEndpoints.map(_._2)).toList ++ swaggerEndpoints ++ List(
      commonRoute.staticPage,
      commonRoute.staticDefault
    )
  )
}
