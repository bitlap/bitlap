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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ListBuffer
import scala.reflect.{ classTag, ClassTag }
import scala.util.{ Failure, Success, Try }

/** Event Bus system.
 */

trait BitlapEvent
type BitlapSubscriber[A] = A => Unit

class EventBus(val executor: ExecutorService = Executors.newWorkStealingPool()) extends LifeCycleWrapper {
  private val subscribers = ConcurrentHashMap[Class[_ <: BitlapEvent], ListBuffer[BitlapSubscriber[_]]]()

  def subscribe[A <: BitlapEvent:ClassTag](subscriber: BitlapSubscriber[A]): EventBus = {
    val subscribers = this.subscribers
      .computeIfAbsent(classTag[A].runtimeClass.asInstanceOf[Class[_ <: BitlapEvent]], { _ => ListBuffer() })
    subscribers += subscriber.asInstanceOf[BitlapSubscriber[_]]
    this
  }

  def post[A <: BitlapEvent](event: A): EventBus = {

    Option(this.subscribers.get(event.getClass)) match {
      case Some(subs) =>
        subs.foreach { sub =>
          executor.execute(() => {
            sub.asInstanceOf[BitlapSubscriber[A]].apply(event)
          })
        }
      case None =>

    }
    this
  }

  override def start(): Unit = synchronized {
    super.start()
    log.info(s"EventBus system has been started.")
  }

  override def close(): Unit = synchronized {
    super.close()
    if (this.executor.isShutdown) {
      return
    }

    Try {
      this.executor.shutdown()
      if (!this.executor.awaitTermination(2, TimeUnit.SECONDS)) {
        this.executor.shutdownNow()
      }
    } match {
      case Failure(e) =>
        log.error(s"Error when closing EventBus, cause: ", e)
      case _ =>
    }

    log.info(s"EventBus system has been closed.")
  }
}
