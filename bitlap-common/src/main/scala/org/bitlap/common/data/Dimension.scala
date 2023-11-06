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
package org.bitlap.common.data

/** Common dimension in [[Event]]
 */
case class Dimension(private val pairs: Map[String, String] = Map.empty) {

  lazy val dimensions = scala.collection.mutable.SortedMap[String, String](pairs.toList: _*)

  def this(pairs: (String, String)*) = this(pairs.toMap)

  def add(pairs: (String, String)*): Dimension = {
    pairs.foreach { case (first, second) => this.dimensions.put(first, second) }
    this
  }

  def apply(key: String): String = {
    this.dimensions(key)
  }

  def update(key: String, value: String): Dimension = {
    this.dimensions.put(key, value)
    this
  }

  def firstPair(): (String, String) = {
    this.dimensions.head
  }

  override def toString: String = dimensions.toString()
}
