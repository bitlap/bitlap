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
package org.bitlap.common.data

import java.io.Serializable

/**
 * Common event.
 *
 * In user behavior analysis, each user action can be abstracted into an event stream
 *
 */
interface Event : Serializable {
    val time: Long
    val entity: Entity
    val dimension: Dimension
    val metric: Metric

    companion object {
        // event schema
        @JvmStatic
        val schema = listOf(
            "time" to "long",
            "entity" to "integer",
            "dimensions" to "string", // json, map<string, string>
            "metric_name" to "string",
            "metric_value" to "double"
        )

        @JvmStatic
        fun of(time: Long, entity: Entity, dimension: Dimension, metric: Metric) =
            EventImpl(time, entity, dimension, metric)
    }
}

/**
 * simple event
 */
data class EventImpl(
    override val time: Long,
    override val entity: Entity,
    override val dimension: Dimension,
    override val metric: Metric,
) : Event

/**
 * Event with dimension sort id, [metric] and [dimId] is mutable
 */
data class EventWithDimId(
    override val time: Long,
    override val entity: Entity,
    override val dimension: Dimension,
    override var metric: Metric,
    var dimId: Int = 0
) : Event {

    companion object {
        @JvmStatic
        fun from(e: Event, withMetricValue: Boolean = true) =
            if (withMetricValue) {
                EventWithDimId(e.time, e.entity, e.dimension, e.metric)
            } else {
                EventWithDimId(e.time, e.entity, e.dimension, Metric(e.metric.key))
            }
    }
}
