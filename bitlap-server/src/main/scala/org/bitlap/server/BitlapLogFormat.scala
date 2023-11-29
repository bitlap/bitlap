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
package org.bitlap.server

import zio.{ Runtime, Trace }
import zio.logging.{ LogColor, LogFilter, LogFormat }
import zio.logging.LogFormat.{ quoted, * }
import zio.logging.backend.SLF4J

object BitlapLogFormat {

  private val traceFormat = LogFormat.make {
    (
      builder,
      trace,
      _,
      _,
      _,
      _,
      _,
      _,
      _
    ) =>
      val text = trace match
        case Trace(location, file, line) => s" $location:$line"
        case t                           => s" $t"
      builder.appendText(text)
  }

  // we don't need to print the time and log level in zio anymore because they already exist in log4j2.
  val colored: LogFormat =
    label("thread", fiberId).color(LogColor.WHITE) |-|
      label("message", quoted(line)).highlight |-|
      label("trace", traceFormat).color(LogColor.BLUE) +
      (space + label("cause", cause).highlight).filter(LogFilter.causeNonEmpty)

  val slf4j = Runtime.removeDefaultLoggers >>> SLF4J.slf4j(colored)

}
