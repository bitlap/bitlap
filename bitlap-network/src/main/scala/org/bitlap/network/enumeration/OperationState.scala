/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network.enumeration

import enumeratum.values.*
import org.bitlap.network.NetworkException.IllegalStateException
import org.bitlap.network.driver_proto.*
import izumi.reflect.dottyreflection.*

/** bitlap客户端操作的状态
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
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

  def toOperationState(bOperationState: BOperationState): OperationState =
    OperationState.withValueOpt(bOperationState.value).getOrElse(UnknownState)

  def toBOperationState(operationState: OperationState): BOperationState =
    BOperationState.fromValue(operationState.value)

  implicit final class ValidateTransition(val state: OperationState) extends AnyVal:
    def validateTransition(newState: OperationState): Unit =
      validate(state, newState)

  def validate(oldState: OperationState, newState: OperationState): Unit =
    oldState match
      case InitializedState =>
        newState match
          case PendingState  =>
          case RunningState  =>
          case CanceledState =>
          case ClosedState   =>
          case TimeoutState  => ()
          case _ => throw IllegalStateException(s"Illegal Operation state transition from $oldState to $newState")

      case PendingState =>
        newState match
          case RunningState  =>
          case FinishedState =>
          case CanceledState =>
          case ErrorState    =>
          case ClosedState   =>
          case TimeoutState  => ()
          case _ => throw IllegalStateException(s"Illegal Operation state transition from $oldState to $newState")

      case RunningState =>
        newState match
          case FinishedState =>
          case CanceledState =>
          case ErrorState    =>
          case ClosedState   =>
          case TimeoutState  => ()
          case _ => throw IllegalStateException(s"Illegal Operation state transition from $oldState to $newState")

      case FinishedState                         =>
      case CanceledState                         =>
      case TimeoutState                          =>
      case ErrorState if ClosedState == newState => ()
      case _ =>
        throw IllegalStateException(s"Illegal Operation state transition from $oldState to $newState")
