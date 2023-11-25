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
package org.bitlap.jdbc

/** bitlap constants for clients
 */
object Constants:

  /** Driver name
   */
  val NAME = "Bitlap JDBC Driver"

  /** Major version number of this driver.
   */
  val MAJOR_VERSION = 0

  /** Minor version number of this driver.
   */
  val MINOR_VERSION = 0

  /** Is this driver JDBC compliant?
   */
  val JDBC_COMPLIANT = false

  /** The required prefix for the connection url
   */
  val URL_PREFIX = "jdbc:bitlap://"

  /** If host is provided, without a port
   */
  val DEFAULT_PORT = "23333"

  /** Property key for the database name
   */
  val DBNAME_PROPERTY_KEY = "DBNAME"

  /** Property key for the bitlap Server host
   */
  val HOST_PROPERTY_KEY = "HOST"

  /** Property key for the bitlap Server port
   */
  val PORT_PROPERTY_KEY = "PORT"

  val DEFAULT_DB = "default"

  val URI_JDBC_PREFIX = "jdbc:"

  val FETCH_SIZE = 1000

  val QUERY_TIMEOUT_SECONDS = 60

  // ===========================================================================================
  // for properties, bitlapconf:key -> value
  final val BITLAP_CONF_PREFIX     = "bitlapconf:"
  final val BITLAP_INIT_SQL        = "initFile"
  final val BITLAP_RETRIES         = "retries"
  final val BITLAP_DEFAULT_RETRIES = 1
