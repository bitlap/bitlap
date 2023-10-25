/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.catalog.metadata

/** Database metadata
 */
case class Database(private val _name: String) {
  val name: String = _name.toLowerCase()
}

object Database {
  val DEFAULT_DATABASE = "default"
}
