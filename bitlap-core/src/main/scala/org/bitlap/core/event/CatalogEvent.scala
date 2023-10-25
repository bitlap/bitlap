/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.event

import org.bitlap.common.BitlapEvent
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.catalog.metadata.Table

// TODO (Add event status, exception, and etc.)

// database
case class DatabaseCreateEvent(database: Database) extends BitlapEvent
case class DatabaseDeleteEvent(database: Database) extends BitlapEvent

// table
case class TableCreateEvent(table: Table) extends BitlapEvent
case class TableDeleteEvent(table: Table) extends BitlapEvent
