package org.bitlap.common.data

import java.io.Serializable

/**
 * Desc: Common event.
 *
 * In user behavior analysis, each user action can be abstracted into an event stream
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/8
 */
interface Event : Serializable {
    val time: Long
    val entity: Entity
    val dimension: Dimension
    val metric: Metric

    companion object {
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
        fun from(e: Event, withMetricValue: Boolean = true) =
            if (withMetricValue) {
                EventWithDimId(e.time, e.entity, e.dimension, e.metric)
            } else {
                EventWithDimId(e.time, e.entity, e.dimension, Metric(e.metric.key))
            }
    }
}
