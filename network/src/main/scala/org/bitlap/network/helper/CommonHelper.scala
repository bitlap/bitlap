package org.bitlap.network.helper

import org.bitlap.network.NetworkHelper

trait CommonHelper extends NetworkHelper {

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
