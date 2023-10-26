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
package org.bitlap.network.enumeration

import org.bitlap.network.Driver.*

import enumeratum.values.*

/** Bitlap client operation type
 */
sealed abstract class OperationType(val value: Int) extends IntEnumEntry

object OperationType extends IntEnum[OperationType]:

  case object UnknownOperation extends OperationType(0)
  case object ExecuteStatement extends OperationType(1)

  val values: IndexedSeq[OperationType] = findValues

  def toOperationType(operationType: BOperationType): OperationType =
    OperationType.withValueOpt(operationType.value).getOrElse(UnknownOperation)
