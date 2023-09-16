/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.jdbc

import scala.collection.immutable.ListMap

/** Bitlap connection parameters
 *
 *  @author
 *    梦境迷离
 *  @since 2023/3/11
 *  @version 1.0
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
