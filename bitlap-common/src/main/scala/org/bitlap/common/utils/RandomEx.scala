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
package org.bitlap.common.utils

import scala.util.Random

/** Random extension utils
 */
object RandomEx {

  private val BASE_NUMBER      = "0123456789"
  private val BASE_CHAR        = "abcdefghijklmnopqrstuvwxyz"
  private val BASE_CHAR_NUMBER = BASE_CHAR + BASE_NUMBER

  def string(limit: Int): String = {
    PreConditions.checkExpression(limit > 0)
    (0 until limit).map { _ => BASE_CHAR_NUMBER(Random.nextInt(BASE_CHAR_NUMBER.length)) }
      .mkString("")
  }
}
