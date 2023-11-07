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
package org.bitlap.common.conf

object Validators {

  val NOT_BLANK: Validator[String] = (t: String) => t != null && !t.isBlank

  def eq[T: Ordering](v: T): Validator[T] = (t: T) => t != null && implicitly[Ordering[T]].compare(t, v) == 0

  def neq[T: Ordering](v: T): Validator[T] = (t: T) => t != null && implicitly[Ordering[T]].compare(t, v) != 0

  def gt[T: Ordering](v: T): Validator[T] = (t: T) => t != null && implicitly[Ordering[T]].gt(t, v)

  def gte[T: Ordering](v: T): Validator[T] = (t: T) => t != null && implicitly[Ordering[T]].gteq(t, v)

  def lt[T: Ordering](v: T): Validator[T] = (t: T) => t != null && implicitly[Ordering[T]].lt(t, v)

  def lte[T: Ordering](v: T): Validator[T] = (t: T) => t != null && implicitly[Ordering[T]].lteq(t, v)
}
