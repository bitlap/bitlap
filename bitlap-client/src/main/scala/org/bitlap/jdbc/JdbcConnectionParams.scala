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

import scala.collection.immutable.ListMap

/** Bitlap connection parameters
 */
private[jdbc] class JdbcConnectionParams:
  private[this] var _dbName: String                       = Constants.DEFAULT_DB
  private[this] var _bitlapConfs: ListMap[String, String] = ListMap[String, String]()
  private[this] var _sessionVars: ListMap[String, String] = ListMap[String, String]()
  private[this] var __authorityList: Array[String]        = Array.empty

  def dbName: String = _dbName

  def dbName_=(value: String): Unit =
    _dbName = value

  def bitlapConfs: ListMap[String, String] = _bitlapConfs

  def bitlapConfs_=(value: ListMap[String, String]): Unit =
    _bitlapConfs = value

  def sessionVars: ListMap[String, String] = _sessionVars

  def sessionVars_=(value: ListMap[String, String]): Unit =
    _sessionVars = value

  def authorityList: Array[String] = __authorityList

  def authorityList_=(value: Array[String]): Unit =
    __authorityList = value

object JdbcConnectionParams:

  // For a jdbc url: jdbc:bitlap://<host>:<port>/dbName;sess_var_list?bitlap_conf_list
  private[jdbc] val AUTH_TYPE   = "auth"
  private[jdbc] val AUTH_USER   = "user"
  private[jdbc] val AUTH_PASSWD = "password"
