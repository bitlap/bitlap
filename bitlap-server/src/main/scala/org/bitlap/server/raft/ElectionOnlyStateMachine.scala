/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.alipay.sofa.jraft.core._
import com.alipay.sofa.jraft.{ Iterator => JRIterator, Status }
import org.slf4j._

import java.util.concurrent.atomic._
import java.util.{ ArrayList => JArrayList, List => JList }
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final class ElectionOnlyStateMachine extends StateMachineAdapter {

  private val LOG = LoggerFactory.getLogger(classOf[ElectionOnlyStateMachine])

  private val leaderTerm                            = new AtomicLong(-1L)
  private var listeners: JList[LeaderStateListener] = new JArrayList[LeaderStateListener]

  def this(listeners: JList[LeaderStateListener]) {
    this()
    this.listeners = listeners
  }

  override def onApply(it: JRIterator): Unit =
    while (it.hasNext) {
      LOG.info("On apply with term: {} and index: {}. ", it.getTerm, it.getIndex)
      it.next
    }

  override def onLeaderStart(term: Long): Unit = {
    super.onLeaderStart(term)
    this.leaderTerm.set(term)
    for (listener <- this.listeners.asScala) // iterator the snapshot
      listener.onLeaderStart(term)
  }

  override def onLeaderStop(status: Status): Unit = {
    super.onLeaderStop(status)
    val oldTerm = leaderTerm.get
    this.leaderTerm.set(-1L)
    for (listener <- this.listeners.asScala)
      listener.onLeaderStop(oldTerm)
  }

  def isLeader: Boolean = this.leaderTerm.get > 0

  def addLeaderStateListener(listener: LeaderStateListener): Unit =
    this.listeners.add(listener)
}
