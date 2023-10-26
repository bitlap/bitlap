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
