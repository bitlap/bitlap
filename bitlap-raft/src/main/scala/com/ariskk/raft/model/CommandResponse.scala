/* Copyright (c) 2022 bitlap.org */
package com.ariskk.raft.model

sealed trait CommandResponse
object CommandResponse {
  case object Committed                 extends CommandResponse
  case class Redirect(leaderId: NodeId) extends CommandResponse
  case object LeaderNotFoundResponse    extends CommandResponse
}
