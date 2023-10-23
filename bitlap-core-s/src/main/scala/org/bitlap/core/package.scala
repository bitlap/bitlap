package org.bitlap

package object core {
  
  object extension {
    extension[T] (o: T) {

      def also(block: T => Unit): T = {
        block(o)
        o
      }

      def let[R](block: T => R): R = {
        block(o)
      }
    }
    
    extension [R <: AutoCloseable](rs: R) {
      def use[T](func: R => T): T = {
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

  @inline def elapsed[T](f: => T): Long = this.elapsedWith(f)._1

  @inline def elapsedWith[T](f: => T): (Long, T) = {
    val start = System.nanoTime()
    val ret = f
    ((System.nanoTime() - start) / 1000000L, ret)
  }
}
