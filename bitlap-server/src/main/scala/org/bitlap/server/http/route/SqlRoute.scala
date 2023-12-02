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
import org.bitlap.server.http.model.{ SqlData, SqlInput }
import org.bitlap.server.http.service.SqlService

import io.circe.*
import io.circe.generic.auto.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.ztapir.*
import zio.*

object SqlRoute:

  lazy val live: ZLayer[SqlService, Nothing, SqlRoute] =
    ZLayer.fromFunction((sqlService: SqlService) => SqlRoute(sqlService))
end SqlRoute

/** Routes for bitlap sql endpoints.
 */
final class SqlRoute(sqlService: SqlService) extends PublicRoute("sql") {

  post(_.in("run").in(jsonBody[SqlInput]).out(jsonBody[Response[SqlData]])) { in =>
    sqlService.execute(in.sql).response
  }
}
