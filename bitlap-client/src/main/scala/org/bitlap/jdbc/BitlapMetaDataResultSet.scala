/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

/** @author
 *    梦境迷离
 *  @since 2021/8/23
 *  @version 1.0
 */
abstract class BitlapMetaDataResultSet[M](
  override val columnNames: List[String] = Nil,
  override val columnTypes: List[String] = Nil,
  protected var data: List[M] = Nil
) extends BitlapBaseResultSet
