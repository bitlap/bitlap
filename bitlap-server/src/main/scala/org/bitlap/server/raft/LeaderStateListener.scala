/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.raft

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
trait LeaderStateListener {

  /** Called when current node becomes leader
   */
  def onLeaderStart(leaderTerm: Long): Unit

  /** Called when current node loses leadership.
   */
  def onLeaderStop(leaderTerm: Long): Unit

}
