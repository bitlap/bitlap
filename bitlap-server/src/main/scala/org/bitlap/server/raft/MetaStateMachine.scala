/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.alipay.sofa.jraft.core.StateMachineAdapter

import com.alipay.sofa.jraft
import com.alipay.sofa.jraft.Status

import java.util.concurrent.atomic.AtomicLong

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class MetaStateMachine extends StateMachineAdapter {

  private val leaderTerm = new AtomicLong(-1)

  override def onApply(iterator: jraft.Iterator): Unit =
    while (iterator.hasNext)
      iterator.next()

  override def onLeaderStart(term: Long) {
    super.onLeaderStart(term)
    this.leaderTerm.set(term)
  }

  override def onLeaderStop(status: Status) {
    super.onLeaderStop(status)
    this.leaderTerm.set(-1L)
  }
}
