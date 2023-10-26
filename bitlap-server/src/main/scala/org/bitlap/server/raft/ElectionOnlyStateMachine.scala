/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server.raft

import java.util.{ ArrayList as JArrayList, List as JList }
import java.util.concurrent.atomic.*

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.slf4j.*

import com.alipay.sofa.jraft.{ Iterator as JRIterator, Status }
import com.alipay.sofa.jraft.core.*

/** Raft state machine, not currently in use
 */
final class ElectionOnlyStateMachine extends StateMachineAdapter:

  private lazy val LOG = LoggerFactory.getLogger(classOf[ElectionOnlyStateMachine])

  private val leaderTerm                            = new AtomicLong(-1L)
  private var listeners: JList[LeaderStateListener] = new JArrayList[LeaderStateListener]

  def this(listeners: JList[LeaderStateListener]) = {
    this()
    this.listeners = listeners
  }

  override def onApply(it: JRIterator): Unit =
    while it.hasNext do {
      LOG.info("On apply with term: {} and index: {}. ", it.getTerm, it.getIndex)
      it.next
    }

  override def onLeaderStart(term: Long): Unit = {
    super.onLeaderStart(term)
    this.leaderTerm.set(term)
    for listener <- this.listeners.asScala do // iterator the snapshot
      listener.onLeaderStart(term)
  }

  override def onLeaderStop(status: Status): Unit = {
    super.onLeaderStop(status)
    val oldTerm = leaderTerm.get
    this.leaderTerm.set(-1L)
    for listener <- this.listeners.asScala do listener.onLeaderStop(oldTerm)
  }

  def isLeader: Boolean = this.leaderTerm.get > 0

  def addLeaderStateListener(listener: LeaderStateListener): Unit =
    this.listeners.add(listener)
