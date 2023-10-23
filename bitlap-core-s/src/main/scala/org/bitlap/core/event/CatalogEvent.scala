/** Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.event

import org.bitlap.common.BitlapEvent
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.catalog.metadata.Table

// TODO (Add event status, exception, and etc.)

// database
case class DatabaseCreateEvent(val database: Database) extends BitlapEvent
case class DatabaseDeleteEvent(val database: Database) extends BitlapEvent

// table
case class TableCreateEvent(val table: Table) extends BitlapEvent
case class TableDeleteEvent(val table: Table) extends BitlapEvent
