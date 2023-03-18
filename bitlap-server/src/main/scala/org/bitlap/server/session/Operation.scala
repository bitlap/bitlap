/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import com.typesafe.scalalogging.LazyLogging
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.models._
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import org.bitlap.network.enumeration.OperationState._
import java.util.concurrent.CountDownLatch
import org.bitlap.common.BitlapConf
import org.bitlap.network.enumeration.{ OperationState, OperationType }

/** bitlap 操作
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
abstract class Operation(
  val parentSession: Session,
  val opType: OperationType,
  val hasResultSet: Boolean = false
) extends LazyLogging {

  @volatile var state: OperationState = OperationState.InitializedState
  val beginTime                       = System.currentTimeMillis()
  @volatile var lastAccessTime        = beginTime
  var operationTimeout                = parentSession.sessionConf.get(BitlapConf.NODE_RPC_TIMEOUT)
  var statement: String               = _

  lazy val opHandle: OperationHandle                = new OperationHandle(opType, hasResultSet)
  lazy val confOverlay: mutable.Map[String, String] = mutable.HashMap[String, String]()

  protected var operationStart    = 0L
  protected var operationComplete = 0L
  protected lazy val cache: mutable.HashMap[OperationHandle, QueryResult] =
    mutable.HashMap()

  private lazy val opTerminateMonitorLatch: CountDownLatch = new CountDownLatch(1)

  def run(): Unit

  def remove(operationHandle: OperationHandle) {
    cache.remove(operationHandle)
  }

  def getNextResultSet(): RowSet =
    cache.get(opHandle).map(_.rows).getOrElse(RowSet())

  def getResultSetSchema(): TableSchema =
    cache.get(opHandle).map(_.tableSchema).getOrElse(TableSchema())

  def setState(operationState: OperationState): OperationState = {
    state.validateTransition(operationState)
    val prevState = state
    this.lastAccessTime = System.currentTimeMillis
    state = operationState
    onNewState(state)
    state
  }

  protected def onNewState(state: OperationState): Unit = {
    state match {
      case RunningState =>
        markOperationStartTime()
      case ErrorState    =>
      case FinishedState =>
      case CanceledState =>
        markOperationCompletedTime()
      case _ =>
    }
    if (state.terminal) { // Unlock the execution thread as operation is already terminated.
      opTerminateMonitorLatch.countDown()
    }
  }

  protected def markOperationStartTime(): Unit =
    operationStart = System.currentTimeMillis

  protected def markOperationCompletedTime(): Unit =
    operationComplete = System.currentTimeMillis

  /** Wait until the operation terminates and returns false if timeout.
   *
   *  @param timeOutMs
   *    \- timeout in milli-seconds
   *  @return
   *    true if operation is terminated or false if timed-out
   *  @throws InterruptedException
   */
  @throws[InterruptedException]
  def waitToTerminate(timeOutMs: Long): Boolean = opTerminateMonitorLatch.await(timeOutMs, TimeUnit.MILLISECONDS)

  def isTimedOut(current: Long): Boolean = {
    if (operationTimeout == 0) return false
    if (operationTimeout > 0) { // check only when it's in terminal state
      return state.terminal && lastAccessTime + operationTimeout <= current
    }
    lastAccessTime + -operationTimeout <= current
  }

}
