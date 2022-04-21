/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
trait NetworkHelper {

  /**
   * get databases or schemas from catalog
   *
   * @see `show databases`
   */
  def getDatabases(pattern: String): List[String]

  /**
   * get tables from catalog with database name
   *
   * @see `show tables in [db_name]`
   */
  def getTables(database: String, pattern: String): List[String]
}
