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

import java.net.*
import java.util
import java.util.ArrayList as JArrayList
import java.util.regex.Pattern

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*
import scala.util.control.Breaks.*

import org.bitlap.common.exception.BitlapSQLException
import org.bitlap.jdbc.Constants.*

object Utils:

  def parseUri(_uri: String): JdbcConnectionParams =
    var uri        = _uri
    val connParams = new JdbcConnectionParams
    if !uri.startsWith(URL_PREFIX) then throw BitlapSQLException(s"Bad URL format: Missing prefix " + URL_PREFIX)
    val dummyAuthorityString = "dummyhost:00000"
    val suppliedAuthorities  = getAuthorities(uri)
    val authorityList        = suppliedAuthorities.split(",")
    connParams.authorityList = authorityList
    uri = uri.replace(suppliedAuthorities, dummyAuthorityString)

    // Now parse the connection uri with dummy authority
    val jdbcURI = URI.create(uri.substring(URI_JDBC_PREFIX.length))
    // key=value pattern
    val pattern  = Pattern.compile("([^;]*)=([^;]*)[;]?")
    var sessVars = jdbcURI.getPath
    if (sessVars != null) && sessVars.nonEmpty then
      var dbName = ""
      // removing leading '/' returned by getPath()
      sessVars = sessVars.substring(1)
      if !sessVars.contains(";") then dbName = sessVars
      else
        dbName = sessVars.substring(0, sessVars.indexOf(';'))
        sessVars = sessVars.substring(sessVars.indexOf(';') + 1)
        if sessVars != null then
          val sessMatcher = pattern.matcher(sessVars)
          while sessMatcher.find do
            if sessMatcher.group(2) != null then
              connParams.sessionVars = connParams.sessionVars ++ ListMap(sessMatcher.group(1) -> sessMatcher.group(2))
            else throw BitlapSQLException("Bad URL format: Multiple values for property " + sessMatcher.group(1))
      if dbName.nonEmpty then connParams.dbName = dbName
    val confStr = jdbcURI.getQuery
    if confStr != null then
      val confMatcher = pattern.matcher(confStr)
      while confMatcher.find do
        connParams.bitlapConfs = connParams.bitlapConfs ++ ListMap(confMatcher.group(1) -> confMatcher.group(2))

    connParams

  private def getAuthorities(uri: String): String =

    /** For a jdbc uri like: jdbc:bitlap://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?conf_list Extract the
     *  uri host:port list starting after "jdbc:bitlap://", till the 1st "/" or "?" whichever comes first & in the given
     *  order Examples:
     *
     *  jdbc:bitlap://host1:port1,host2:port2,host3:port3/db;k1=v1?k2=v2
     *
     *  jdbc:bitlap://host1:port1,host2:port2,host3:port3/;k1=v1?k2=v2
     *
     *  jdbc:bitlap://host1:port1,host2:port2,host3:port3?k2=v2
     *
     *  jdbc:bitlap://host1:port1,host2:port2,host3:port3
     */
    val fromIndex: Int             = URL_PREFIX.length
    var toIndex: Int               = -1
    val toIndexChars: List[String] = new util.ArrayList[String](util.Arrays.asList("/", "?")).asScala.toList

    breakable {
      for toIndexChar <- toIndexChars do
        toIndex = uri.indexOf(toIndexChar, fromIndex)
        if toIndex > 0 then break()
    }

    if toIndex < 0 then uri.substring(fromIndex)
    else uri.substring(fromIndex, toIndex)

  def parseInitFile(initFile: String): List[String] = org.bitlap.common.utils.StringEx.parseInitFile(initFile)
