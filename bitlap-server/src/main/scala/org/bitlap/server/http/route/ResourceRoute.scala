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

import org.bitlap.server.http.Response

import io.circe.*
import io.circe.generic.auto.*
import sttp.tapir.files.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.ztapir.*
import zio.*

object ResourceRoute:
  lazy val live: ULayer[ResourceRoute] = ZLayer.succeed(ResourceRoute())
end ResourceRoute

/** Routes for bitlap static endpoints.
 */
final class ResourceRoute extends PublicRoute("common") {

  private val classLoader = ResourceRoute.getClass.getClassLoader

  get(_.in("status").out(jsonBody[Response[String]])) { _ =>
    ZIO.succeed(Response.ok("ok"))
  }

  val staticPage: ServerEndpoint[Any, Task] =
    staticResourceGetServerEndpoint[Task]("pages")(classLoader, "static/index.html")

  val staticDefault: ServerEndpoint[Any, Task] =
    staticResourcesGetServerEndpoint[Task](emptyInput)(classLoader, "static")
}
