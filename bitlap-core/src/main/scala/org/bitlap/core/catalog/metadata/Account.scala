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
package org.bitlap.core.catalog.metadata

import Account._

final case class Account(private val _name: String, secretKey: SecretKey = SecretKey(DEFAULT_PASSWORD)) {
  val name: String = _name.toLowerCase()
}

object Account {
  val DEFAULT_USER     = "root"
  val DEFAULT_PASSWORD = ""
  val DEFAULT_DIR      = "/account"

  final case class SecretKey(value: String = DEFAULT_PASSWORD)
}
