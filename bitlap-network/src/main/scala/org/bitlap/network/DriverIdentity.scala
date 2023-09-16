/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.network

import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** Functional synchronous RPC API, both for client and server, logic should be delegated to asynchronous RPC
 *  [[org.bitlap.network.DriverIO]] and should not be implemented on its own.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait DriverIdentity extends DriverX[Identity]:
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

  def cancelOperation(opHandle: OperationHandle): Identity[Unit]

  def getOperationStatus(opHandle: OperationHandle): Identity[OperationStatus]
