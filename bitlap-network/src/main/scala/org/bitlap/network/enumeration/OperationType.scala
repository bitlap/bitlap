/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network.enumeration

import org.bitlap.network.driver_proto.*

import enumeratum.values.*

/** bitlap客户端操作类型
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
sealed abstract class OperationType(val value: Int) extends IntEnumEntry

object OperationType extends IntEnum[OperationType]:

  final case object UnknownOperation extends OperationType(0)
  final case object ExecuteStatement extends OperationType(1)
  final case object GetSchemas       extends OperationType(2)
  final case object GetTables        extends OperationType(3)

  val values: IndexedSeq[OperationType] = findValues

  def toOperationType(bOperationType: BOperationType): OperationType =
    OperationType.withValueOpt(bOperationType.value).getOrElse(UnknownOperation)
