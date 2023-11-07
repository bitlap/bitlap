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
package org.bitlap.common.exception

import scala.util.Try

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/** get errors info from classpath `bitlap-errors.conf`
 */
lazy val errors: Config = {
  ConfigFactory.load("bitlap-errors")
}

extension (errorKey: String) {

  /** format message of [this] key path with [parameter]
   */
  def formatErrorMessage(parameter: Map[String, String]): String = {
    if (errorKey == null || errorKey.isBlank) return ""

    Try {
      errors.getConfig(errorKey)
    }.map { key =>
      parameter.foldLeft(key.getString("message")) { case (acc, (k, v)) =>
        acc.replace(s"<$k>", v)
      }
    }.getOrElse(errorKey)
  }

  /** format sql state of [this] key path
   */
  def formatSqlState(): String = {
    if (errorKey == null || errorKey.isBlank) return ""

    Try {
      errors.getConfig(errorKey)
    }.map(_.getString("sqlState"))
      .getOrElse("")
  }
}
