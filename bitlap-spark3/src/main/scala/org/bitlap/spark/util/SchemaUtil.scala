/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.util

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/16
 */
object SchemaUtil {

  val NAME_SEPARATOR   = "."
  val ESCAPE_CHARACTER = "\""

  /** Normalize an identifier. If name is surrounded by double quotes, the double quotes are stripped and the rest is
   *  used as-is, otherwise the name is upper caased.
   *
   *  @param name
   *    the parsed identifier
   *  @return
   *    the normalized identifier
   */
  def normalizeIdentifier(name: String): String = {
    if name == null then return name
    if isCaseSensitive(name) then { // Don't upper case if in quotes
      return name.substring(1, name.length - 1)
    }
    name.toUpperCase
  }

  def isCaseSensitive(name: String): Boolean = name != null && name.nonEmpty && name.charAt(0) == '"'

  def getEscapedArgument(argument: String): String = {
    if argument == null then {
      throw new NullPointerException("Argument passed cannot be null")
    }
    ESCAPE_CHARACTER + argument + ESCAPE_CHARACTER
  }

  def getEscapedFullColumnName(fullColumnName: String): String = {
    if fullColumnName.startsWith(ESCAPE_CHARACTER) then return fullColumnName
    val index = fullColumnName.indexOf(NAME_SEPARATOR)
    if index < 0 then return getEscapedArgument(fullColumnName)
    val columnFamily = fullColumnName.substring(0, index)
    val columnName   = fullColumnName.substring(index + 1)
    getEscapedArgument(columnFamily) + NAME_SEPARATOR + getEscapedArgument(columnName)
  }
}
