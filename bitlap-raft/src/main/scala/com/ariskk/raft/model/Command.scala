/* Copyright (c) 2022 bitlap.org */
package com.ariskk.raft.model

sealed trait Command extends Serializable
object Command {
  trait ReadCommand extends Command

  trait WriteCommand extends Command
}
