/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.operation

import com.typesafe.scalalogging.LazyLogging
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.models
import org.bitlap.network.models.RowSet
import org.bitlap.network.operation.OperationType.OperationType
import org.bitlap.network.session.Session

import scala.collection.mutable

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
object operations {

  abstract class Operation(
    val parentSession: Session,
    val opType: OperationType,
    val hasResultSet: Boolean = false
  ) extends LazyLogging {

    private var statement: String = _

    // super不能用于字段
    def getStatement: String = statement

    def setStatement(statt: String): Unit =
      statement = statt

    def getOpHandle: OperationHandle = opHandle

    lazy val opHandle: OperationHandle =
      new OperationHandle(opType, hasResultSet)
    lazy val confOverlay: mutable.Map[String, String] =
      mutable.HashMap[String, String]()

    protected lazy val cache: mutable.HashMap[OperationHandle, models.QueryResult] =
      mutable.HashMap()

    def run()

    def remove(operationHandle: OperationHandle) {
      cache.remove(operationHandle)
    }

    def getNextResultSet(): RowSet =
      cache.get(opHandle).map(_.rows).getOrElse(models.RowSet())

    def getResultSetSchema(): models.TableSchema =
      cache.get(opHandle).map(_.tableSchema).getOrElse(models.TableSchema())
  }

}