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
package org.bitlap.network.enumeration

import org.bitlap.common.exception.BitlapIllegalStateException
import org.bitlap.network.Driver.*

import enumeratum.values.*
import izumi.reflect.dottyreflection.*

/** Status of bitlap client operations
 */
sealed abstract class OperationState(val value: Int, val terminal: Boolean) extends IntEnumEntry

object OperationState extends IntEnum[OperationState]:

  case object UnknownState     extends OperationState(0, false)
  case object InitializedState extends OperationState(1, false)
  case object RunningState     extends OperationState(2, false)
  case object FinishedState    extends OperationState(3, true)
  case object CanceledState    extends OperationState(4, true)
  case object ClosedState      extends OperationState(5, true)
  case object ErrorState       extends OperationState(6, true)
  case object PendingState     extends OperationState(7, false)
  case object TimeoutState     extends OperationState(8, true)

  val values: IndexedSeq[OperationState] = findValues

  def toOperationState(operationState: BOperationState): OperationState =
    OperationState.withValueOpt(operationState.value).getOrElse(UnknownState)

  def toBOperationState(operationState: OperationState): BOperationState =
    BOperationState.fromValue(operationState.value)

  extension (state: OperationState)

    def validateTransition(newState: OperationState): Unit =
      validate(state, newState)
  end extension

  def validate(oldState: OperationState, newState: OperationState): Unit =
    oldState match
      case InitializedState =>
        newState match
          case PendingState  =>
          case RunningState  =>
          case CanceledState =>
          case ClosedState   =>
          case TimeoutState  => ()
          case _ => throw BitlapIllegalStateException(s"Illegal Operation state transition from $oldState to $newState")

      case PendingState =>
        newState match
          case RunningState  =>
          case FinishedState =>
          case CanceledState =>
          case ErrorState    =>
          case ClosedState   =>
          case TimeoutState  => ()
          case _ => throw BitlapIllegalStateException(s"Illegal Operation state transition from $oldState to $newState")

      case RunningState =>
        newState match
          case FinishedState =>
          case CanceledState =>
          case ErrorState    =>
          case ClosedState   =>
          case TimeoutState  => ()
          case _ => throw BitlapIllegalStateException(s"Illegal Operation state transition from $oldState to $newState")

      case FinishedState                         =>
      case CanceledState                         =>
      case TimeoutState                          =>
      case ErrorState if ClosedState == newState => ()
      case _ =>
        throw BitlapIllegalStateException(s"Illegal Operation state transition from $oldState to $newState")
