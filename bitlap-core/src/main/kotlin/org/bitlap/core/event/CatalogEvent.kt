/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.event

import org.bitlap.common.BitlapEvent
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.core.catalog.metadata.Table

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/21
 */

// TODO (Add event status, exception, and etc.)

// database
data class DatabaseCreateEvent(val database: Database) : BitlapEvent
data class DatabaseDeleteEvent(val database: Database) : BitlapEvent

// table
data class TableCreateEvent(val table: Table) : BitlapEvent
data class TableDeleteEvent(val table: Table) : BitlapEvent
