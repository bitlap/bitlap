/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** 函数式同步RPC API，客户端和服务端通用，逻辑应委托给异步RPC[[org.bitlap.network.DriverIO]]，不应自己实现
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait DriverC extends DriverX[Identity]:
  self =>

  def pure[A](a: A): Identity[A] = a

  def map[A, B](fa: self.type => Identity[A])(f: A => B): Identity[B] = f(fa(this))

  def flatMap[A, B](fa: self.type => Identity[A])(f: A => Identity[B]): Identity[B] = f(fa(this))

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Identity[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): Identity[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): Identity[OperationHandle]

  def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): Identity[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema]

  def getDatabases(sessionHandle: SessionHandle, pattern: String): Identity[OperationHandle]

  def getTables(sessionHandle: SessionHandle, database: String, pattern: String): Identity[OperationHandle]

  def cancelOperation(opHandle: OperationHandle): Identity[Unit]

  def getOperationStatus(opHandle: OperationHandle): Identity[OperationStatus]
