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

/**
 * Common metric in [[Event]]
 *
 * [[key]]: which metric the entity does, such as pv, click, order, etc.
 * [[value]]: the metric value, such as count of pv, amount of order, etc.
 */
data class Metric(val key: String, val value: Double = 0.0) {

    operator fun plus(other: Double) = Metric(key, value + other)
}
