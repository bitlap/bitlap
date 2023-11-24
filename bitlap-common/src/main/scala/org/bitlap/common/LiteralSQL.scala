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
package org.bitlap.common

sealed trait LiteralSQL

object LiteralSQL:

  sealed trait ShowSQL(name: String) extends LiteralSQL {
    def command: String = s"SHOW $name"
  }

  final case object ShowCurrentDatabase extends ShowSQL("CURRENT_DATABASE")
  final case object ShowDatabases       extends ShowSQL("DATABASES")
  final case object ShowUsers           extends ShowSQL("USERS")
  final case object ShowTables          extends ShowSQL("TABLES")
