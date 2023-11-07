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

/** Common event.
 *
 *  In user behavior analysis, each user action can be abstracted into an event stream
 */
trait Event extends Serializable {
  val time: Long
  val entity: Entity
  val dimension: Dimension
  val metric: Metric
}

object Event {

  val schema: Seq[(String, String)] = List(
    "time"         -> "long",
    "entity"       -> "integer",
    "dimensions"   -> "string", // json, map<string, string>
    "metric_name"  -> "string",
    "metric_value" -> "double"
  )

  def of(
    time: Long,
    entity: Entity,
    dimension: Dimension,
    metric: Metric
  ): EventImpl =
    EventImpl(time, entity, dimension, metric)
}

/** simple event
 */
case class EventImpl(
  override val time: Long,
  override val entity: Entity,
  override val dimension: Dimension,
  override val metric: Metric)
    extends Event

/** Event with dimension sort id, [metric] and [dimId] is mutable
 */
case class EventWithDimId(
  override val time: Long,
  override val entity: Entity,
  override val dimension: Dimension,
  override val metric: Metric,
  var dimId: Int = 0)
    extends Event

object EventWithDimId {

  def from(e: Event, withMetricValue: Boolean = true): EventWithDimId =
    if (withMetricValue) {
      EventWithDimId(e.time, e.entity, e.dimension, e.metric)
    } else {
      EventWithDimId(e.time, e.entity, e.dimension, Metric(e.metric.key))
    }
}
