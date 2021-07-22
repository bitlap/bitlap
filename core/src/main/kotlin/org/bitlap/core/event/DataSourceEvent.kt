package org.bitlap.core.event

import org.bitlap.common.BitlapEvent

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/21
 */

data class DataSourceCreateEvent(val name: String) : BitlapEvent
data class DataSourceUpdateEvent(val name: String) : BitlapEvent
