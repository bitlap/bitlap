/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network

import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** 函数式RPC API，客户端和服务端通用
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait DriverX[F[_]]:
  self =>

  def pure[A](a: A): F[A]

  def map[A, B](fa: self.type => F[A])(f: A => B): F[B]

  def flatMap[A, B](fa: self.type => F[A])(f: A => F[B]): F[B]

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): F[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): F[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): F[OperationHandle]

  def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): F[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): F[TableSchema]

  def cancelOperation(opHandle: OperationHandle): F[Unit]

  def closeOperation(opHandle: OperationHandle): F[Unit]

  def getOperationStatus(opHandle: OperationHandle): F[OperationStatus]

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): F[GetInfoValue]
