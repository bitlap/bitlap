/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import enumeratum.values._
import org.bitlap.network.driver.proto.BOperationState

/** bitlap客户端操作的状态
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
sealed abstract class OperationState(val value: Int) extends IntEnumEntry
object OperationState extends IntEnum[OperationState] {

  final case object UnknownState     extends OperationState(0)
  final case object InitializedState extends OperationState(1)
  final case object RunningState     extends OperationState(2)
  final case object FinishedState    extends OperationState(3)
  final case object CanceledState    extends OperationState(4)
  final case object ClosedState      extends OperationState(5)
  final case object ErrorState       extends OperationState(6)
  final case object PendingState     extends OperationState(7)

  val values: IndexedSeq[OperationState] = findValues

  def toOperationState(bOperationState: BOperationState): OperationState =
    OperationState.withValueOpt(bOperationState.value).getOrElse(UnknownState)

  def toBOperationState(operationState: OperationState): BOperationState =
    BOperationState.fromValue(operationState.value)

}
