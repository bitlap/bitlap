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

import java.io.File
import java.util.UUID

import scala.util.control.Breaks.{ break, breakable }

object StringEx {

  extension (str: String) {

    /** Fix path, concat [subPaths] with [File.pathSeparator]
     */
    def withPaths(subPaths: String*): String = {
      val separator = File.separator
      subPaths.foldLeft(str) {
        case (p1, p2) if p2.isBlank                                         => p1
        case (p1, p2) if p1.endsWith(separator) && p2.startsWith(separator) => p1 + p2.substring(1)
        case (p1, p2) if p1.endsWith(separator) || p2.startsWith(separator) => p1 + p2
        case (p1, p2)                                                       => p1 + separator + p2
      }
    }

    /** check string is null or blank
     */
    def nullOrBlank: Boolean = str == null || str.isBlank

    /** get [default] if this is blank
     */
    def blankOr(default: String): String = {
      if (str == null || str.isBlank) {
        default
      } else {
        str
      }
    }

    /** trim chars
     */
    def trimMargin(ch: Char*): String = {
      var startIndex = 0
      var endIndex   = str.length - 1
      var startFound = false
      breakable {
        while (startIndex <= endIndex) {
          val index   = if (!startFound) startIndex else endIndex
          val `match` = ch.contains(str(index))

          if (!startFound) {
            if (!`match`)
              startFound = true
            else
              startIndex += 1
          } else {
            if (!`match`)
              break
            else
              endIndex -= 1
          }
        }
      }

      str.substring(startIndex, endIndex + 1)
    }

  }

  def uuid(removeDash: Boolean = false): String = {
    var uuid = UUID.randomUUID().toString
    if (removeDash) {
      uuid = uuid.replace("-", "")
    }
    uuid
  }
}
