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
package org.bitlap.common.utils

import java.io.{ BufferedReader, File, FileInputStream, InputStreamReader, IOException }
import java.util.ArrayList as JArrayList
import java.util.UUID

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.control.Breaks.{ break, breakable }

import org.bitlap.common.exception.BitlapSQLException

import dotty.tools.dotc.semanticdb.internal.MD5

object StringEx {

  extension (str: String) {

    /** Fix path, concat [subPaths] with [File.pathSeparator]
     */
    def withPaths(subPaths: String*): String = {
      val separator = File.separator
      subPaths.foldLeft(str) {
        case (p1, p2) if p2.isBlank                                         => p1
        case (p1, p2) if p1.endsWith(separator) && p2.startsWith(separator) => p1 + p2.substring(1)
        case (p1, p2) if p1.endsWith(separator) || p2.startsWith(separator) => p1 + p2
        case (p1, p2)                                                       => p1 + separator + p2
      }
    }

    /** check string is null or blank
     */
    def nullOrBlank: Boolean = str == null || str.isBlank

    /** get [default] if this is blank
     */
    def blankOr(default: String): String = {
      if (str == null || str.isBlank) {
        default
      } else {
        str
      }
    }

    /** trim chars
     */
    def trimMargin(ch: Char*): String = {
      var startIndex = 0
      var endIndex   = str.length - 1
      var startFound = false
      breakable {
        while (startIndex <= endIndex) {
          val index   = if (!startFound) startIndex else endIndex
          val `match` = ch.contains(str(index))

          if (!startFound) {
            if (!`match`)
              startFound = true
            else
              startIndex += 1
          } else {
            if (!`match`)
              break
            else
              endIndex -= 1
          }
        }
      }

      str.substring(startIndex, endIndex + 1)
    }

  }

  def uuid(removeDash: Boolean = false): String = {
    var uuid = UUID.randomUUID().toString
    if (removeDash) {
      uuid = uuid.replace("-", "")
    }
    uuid
  }

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
          if !line.startsWith("#") && !line.startsWith("--") then {
            line = line.concat(" ")
            sb.append(line)
          }
      initSqlList = getMultipleSqls(sb.toString)
    catch
      case e: IOException =>
        throw BitlapSQLException("Invalid sql syntax in initFile", cause = Option(e))
    finally if br != null then br.close()
    initSqlList

  def getSqlStmts(sqls: String): List[String] = {
    val lines = List(if (sqls.endsWith(";")) sqls else sqls + ";")
    val sb    = new mutable.StringBuilder("")
    lines.map(_.trim).filter(_.nonEmpty).foreach { line =>
      if line.nonEmpty then
        if !line.startsWith("#") && !line.startsWith("--") then {
          sb.append(line.concat(" "))
        }
    }
    getMultipleSqls(sb.toString)

  }

  private def getMultipleSqls(sbLine: String): List[String] =
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
}
