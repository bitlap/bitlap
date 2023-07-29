/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.network.enumeration

import org.bitlap.network.Driver.*

import enumeratum.values.*

/** bitlap客户端操作类型
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
sealed abstract class OperationType(val value: Int) extends IntEnumEntry

object OperationType extends IntEnum[OperationType]:

  case object UnknownOperation extends OperationType(0)
  case object ExecuteStatement extends OperationType(1)

  val values: IndexedSeq[OperationType] = findValues

  def toOperationType(bOperationType: BOperationType): OperationType =
    OperationType.withValueOpt(bOperationType.value).getOrElse(UnknownOperation)
