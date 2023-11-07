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

object extension {

  extension [T](o: T) {

    inline def also(block: T => Unit): T = {
      block(o)
      o
    }

    inline def let[R](block: T => R): R = {
      block(o)
    }
  }

  extension [R <: AutoCloseable](rs: R) {

    inline def use[T](func: R => T): T = {
      try {
        func(rs)
      } finally {
        try {
          rs.close()
        } catch {
          case _: Throwable =>
        }
      }
    }
  }
}

inline def elapsed[T](f: => T): Long = elapsedWith(f)._1

inline def elapsedWith[T](f: => T): (Long, T) = {
  val start = System.nanoTime()
  val ret   = f
  ((System.nanoTime() - start) / 1000000L, ret)
}

/** like sql nvl
 */
inline def nvl[T](t: T, default: T): T = if (t == null) default else t

/** like sql coalesce
 */
inline def coalesce[T](t: T*): T = t.find { _ != null }.get

/** Do `func` when `flag` is true, if `flag` is false, just return `t` only
 */
inline def doIf[T](flag: Boolean, t: T, func: T => T): T = if (flag) func.apply(t) else t
