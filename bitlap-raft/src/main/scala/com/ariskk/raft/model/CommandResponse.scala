/* Copyright (c) 2022 bitlap.org */
package com.ariskk.raft.model

sealed trait CommandResponse
object CommandResponse {
  final case object Committed                       extends CommandResponse
  final case class Redirect(leaderId: NodeId)       extends CommandResponse
  final case object LeaderNotFoundResponse          extends CommandResponse
  final case class QueryResult[+T](data: Option[T]) extends CommandResponse
}
