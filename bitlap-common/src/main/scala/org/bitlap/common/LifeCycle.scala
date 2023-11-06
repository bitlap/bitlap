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
package org.bitlap.common

import java.io.Closeable

import scala.util.{ Failure, Success, Try }

import org.bitlap.common.extension.*

import org.slf4j.{ Logger, LoggerFactory }

trait LifeCycle extends Closeable {
  def start(): Unit
  def isStarted: Boolean
  def isShutdown: Boolean
}

abstract class LifeCycleWrapper extends LifeCycle {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  @volatile
  protected var started = false

  @volatile
  protected var shutdown = true

  override def start(): Unit = {
    shutdown = false
    started = true
  }

  override def close(): Unit = {
    started = false
    shutdown = true
  }

  override def isStarted: Boolean  = this.started
  override def isShutdown: Boolean = this.shutdown
}

abstract class LifeCycleThread(val name: String, val daemon: Boolean) extends LifeCycleWrapper, Runnable {

  def this(name: String) = this(name, false)

  private lazy val thread: Thread = {
    Thread(this, name).also { it =>
      it.setDaemon(daemon)
      it.setUncaughtExceptionHandler { case (t, e) => this.handleException(t, e) }
    }
  }

  protected def handleException(t: Thread, e: Throwable): Unit = {
    log.error(s"Exception occurred from thread ${t.getName}", e)
  }

  override def start(): Unit = synchronized {
    if (this.isStarted) {
      log.info(s"$name has been started.")
      return
    }
    thread.start()
    super.start()
  }

  override def close(): Unit = {
    if (this.isShutdown) {
      log.info(s"$name has been closed.")
      return
    }

    Try {
      if (!thread.isInterrupted) {
        log.info(s"Starting to close thread: $name")
        thread.interrupt()
      }
    } match {
      case Failure(e) =>
        log.warn(s"Error when closing thread: $name", e)
      case _ =>
    }
    super.close()
  }

  def join(): Unit = {
    this.thread.join()
  }
}
