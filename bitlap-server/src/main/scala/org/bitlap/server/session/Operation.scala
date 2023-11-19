/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server.session

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.bitlap.common.BitlapConf
import org.bitlap.network.enumeration.{ OperationState, OperationType }
import org.bitlap.network.enumeration.OperationState.*
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.models.*
import org.bitlap.server.config.BitlapConfiguration

import com.typesafe.scalalogging.LazyLogging

import zio.Task

/** Bitlap operation
 */
abstract class Operation(
  val parentSession: Session,
  val opType: OperationType,
  val hasResultSet: Boolean = false,
  globalConfig: BitlapConfiguration)
    extends LazyLogging {

  @volatile var state: OperationState = OperationState.InitializedState
  @volatile var lastAccessTime: Long  = System.currentTimeMillis()
  var statement: String               = _

  lazy val opHandle: OperationHandle                = OperationHandle(opType, hasResultSet)
  lazy val confOverlay: mutable.Map[String, String] = mutable.HashMap[String, String]()

  protected var operationStart    = 0L
  protected var operationComplete = 0L

  protected lazy val cache: mutable.HashMap[OperationHandle, QueryResultSet] =
    mutable.HashMap()

  private lazy val opTerminateMonitorLatch: CountDownLatch = new CountDownLatch(1)
  private lazy val operationTimeout                        = globalConfig.sessionConfig.sql.toMillis

  def run(): Task[Unit]

  def remove(operationHandle: OperationHandle) =
    cache.remove(operationHandle)

  def getNextResultSet(): RowSet =
    cache.get(opHandle).map(_.rows).getOrElse(RowSet())

  def getResultSetSchema(): TableSchema =
    cache.get(opHandle).map(_.tableSchema).getOrElse(TableSchema())

  def setState(operationState: OperationState): OperationState = this.synchronized {
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
    if state.terminal then { // Unlock the execution thread as operation is already terminated.
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
   */
  def waitToTerminate(timeOutMs: Long): Boolean = opTerminateMonitorLatch.await(timeOutMs, TimeUnit.MILLISECONDS)

  def isTimedOut(current: Long): Boolean = {
    if operationTimeout == 0 then return false
    if operationTimeout > 0 then { // check only when it's in terminal state
      return state.terminal && lastAccessTime + operationTimeout <= current
    }
    lastAccessTime + -operationTimeout <= current
  }

}
