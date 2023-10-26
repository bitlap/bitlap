/**
 * Copyright (C) 2023 bitlap.org .
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
