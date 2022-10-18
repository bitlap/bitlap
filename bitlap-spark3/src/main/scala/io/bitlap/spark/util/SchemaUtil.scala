/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.util

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
    if (name == null) return name
    if (isCaseSensitive(name)) { // Don't upper case if in quotes
      return name.substring(1, name.length - 1)
    }
    name.toUpperCase
  }

  def isCaseSensitive(name: String): Boolean = name != null && name.nonEmpty && name.charAt(0) == '"'

  def getEscapedArgument(argument: String): String = {
    if (argument == null) {
      throw new NullPointerException("Argument passed cannot be null")
    }
    ESCAPE_CHARACTER + argument + ESCAPE_CHARACTER
  }

  def getEscapedFullColumnName(fullColumnName: String): String = {
    if (fullColumnName.startsWith(ESCAPE_CHARACTER)) return fullColumnName
    val index = fullColumnName.indexOf(NAME_SEPARATOR)
    if (index < 0) return getEscapedArgument(fullColumnName)
    val columnFamily = fullColumnName.substring(0, index)
    val columnName   = fullColumnName.substring(index + 1)
    getEscapedArgument(columnFamily) + NAME_SEPARATOR + getEscapedArgument(columnName)
  }
}
