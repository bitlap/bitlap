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
package org.bitlap.jdbc

import java.sql.SQLException

import scala.jdk.CollectionConverters.*

import org.bitlap.common.exception.BitlapException

/** Bitlap SQL exception on client side
 */
final case class BitlapSQLException(
  msg: String,
  state: String = null,
  code: Int = -1,
  cause: Option[Throwable] = None)
    extends SQLException(msg, state, code, cause.orNull)

final case class BitlapJdbcUriParseException(msg: String, cause: Option[Throwable] = None)
    extends BitlapException(msg, Map.empty[String, String].asJava, cause.orNull)
