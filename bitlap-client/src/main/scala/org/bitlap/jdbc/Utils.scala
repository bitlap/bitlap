/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.jdbc.Constants.*

import java.io.*
import java.net.*
import java.util
import java.util.ArrayList as JArrayList
import java.util.regex.Pattern
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.control.Breaks.*

/** @author
 *    梦境迷离
 *  @version 1.0,2023/3/11
 */
object Utils:

  def parseUri(_uri: String): JdbcConnectionParams =
    var uri        = _uri
    val connParams = new JdbcConnectionParams
    if !uri.startsWith(URL_PREFIX) then
      throw BitlapJdbcUriParseException(s"Bad URL format: Missing prefix " + URL_PREFIX)
    if uri.equalsIgnoreCase(URL_PREFIX) then return connParams

    val dummyAuthorityString = "dummyhost:00000"
    val suppliedAuthorities  = getAuthorities(uri)
    println("Supplied authorities: " + suppliedAuthorities)
    val authorityList = suppliedAuthorities.split(",")
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
            else
              throw BitlapJdbcUriParseException("Bad URL format: Multiple values for property " + sessMatcher.group(1))
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

  def parseInitFile(initFile: String): List[String] =
    val file                      = new File(initFile)
    var br: BufferedReader        = null
    var initSqlList: List[String] = Nil
    try
      val input = new FileInputStream(file)
      br = new BufferedReader(new InputStreamReader(input, "UTF-8"))
      var line: String = null
      val sb           = new mutable.StringBuilder("")
      while {
        line = br.readLine
        line != null
      } do
        line = line.trim
        if line.nonEmpty then
          if line.startsWith("#") || line.startsWith("--") then {
            // todo: continue is not supported
          } else
            line = line.concat(" ")
            sb.append(line)
      initSqlList = getInitSql(sb.toString)
    catch
      case e: IOException =>
        throw new IOException(e)
    finally if br != null then br.close()
    initSqlList

  // TODO functional style
  private def getInitSql(sbLine: String): List[String] =
    val sqlArray    = sbLine.toCharArray
    val initSqlList = new JArrayList[String]
    var index       = 0
    var beginIndex  = 0
    while index < sqlArray.length do
      if sqlArray(index) == ';' then
        val sql = sbLine.substring(beginIndex, index).trim
        initSqlList.add(sql)
        beginIndex = index + 1

      index += 1
    initSqlList.asScala.toList
