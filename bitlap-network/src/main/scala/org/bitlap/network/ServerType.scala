/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import enumeratum._

/** @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
private[bitlap] sealed trait ServerType extends EnumEntry

object ServerType extends Enum[ServerType] {

  final case object Grpc extends ServerType
  final case object Raft extends ServerType
  final case object Http extends ServerType

  val values: IndexedSeq[ServerType] = findValues

}
