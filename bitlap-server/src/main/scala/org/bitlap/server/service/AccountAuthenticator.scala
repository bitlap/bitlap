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
package org.bitlap.server.service

import org.bitlap.common.exception.BitlapAuthenticationException
import org.bitlap.core.*
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.enumeration.*
import org.bitlap.network.models.*
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.server.config.BitlapConfiguration
import org.bitlap.server.http.model.AccountInfo
import org.bitlap.server.session.mapTo

import zio.*

object AccountAuthenticator:
  val live = ZLayer.succeed(new AccountAuthenticator)
end AccountAuthenticator

final class AccountAuthenticator extends BitlapSerde {

  def auth(username: String, password: String): ZIO[Any, Throwable, AccountInfo] = {
    val statement: String = s"AUTH $username '$password'"
    val res =
      try {
        val execution = new QueryExecution(statement, Database.DEFAULT_DATABASE).execute()
        execution match
          case DefaultQueryResult(data, _) =>
            data.mapTo.rows.rows.headOption
              .flatMap(_.values.headOption)
              .exists(v => deserialize[Boolean](TypeId.BooleanType, v))
          case _ => false
      } catch {
        case e: Exception =>
          throw BitlapAuthenticationException("Auth failed", cause = Option(e))
      }
      // return detail user info
    ZIO.unless(res)(ZIO.fail(BitlapAuthenticationException("Auth failed"))).unit *>
      ZIO.succeed(AccountInfo.root)
  }

}
