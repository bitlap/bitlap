/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.data

/**
 * Common entity in [[Event]]
 *
 * [[key]: DEVICE, USER, etc.
 * [[id]]: identify of this entity, current only support int type
 */
data class Entity(val id: Int, val key: String = "ENTITY")
