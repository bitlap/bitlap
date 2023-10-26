/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.utils

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper

/** json extension utils
 */
object JsonUtil {

  lazy val mapper: ObjectMapper = ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def json(o: Any): String = {
    mapper.writeValueAsString(o)
  }

  def jsonAs[T](json: String, clazz: Class[T]): T = {
    mapper.readValue(json, clazz)
  }

  def jsonAsMap(json: String): Map[String, String] = {
    if (json == null || json.isBlank) return Map.empty
    mapper.readValue(json, new TypeReference[Map[String, String]]() {})
  }
}
