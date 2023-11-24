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
package org.bitlap.core.event

import org.bitlap.common.BitlapEvent
import org.bitlap.core.catalog.metadata.{ Account, Database, Table }

// TODO (Add event status, exception, and etc.)

// database
final case class DatabaseCreateEvent(database: Database) extends BitlapEvent
final case class DatabaseDeleteEvent(database: Database) extends BitlapEvent

// table
final case class TableCreateEvent(table: Table) extends BitlapEvent
final case class TableDeleteEvent(table: Table) extends BitlapEvent

// user
final case class AccountCreateEvent(account: Account) extends BitlapEvent
final case class AccountDropEvent(account: Account)   extends BitlapEvent
