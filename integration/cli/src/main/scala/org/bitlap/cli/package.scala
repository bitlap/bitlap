package org.bitlap

import picocli.CommandLine

import java.io.{ PrintWriter, StringWriter }
import scala.language.implicitConversions
import scala.reflect.{ classTag, ClassTag }

package object cli {

  val BitlapExecutor = new CommandLine(BitlapCli()) with CliExecutor
  val BitlapServerExecutor = new CommandLine(BitlapServerCli()) with CliExecutor
  val BitlapSqlExecutor = new CommandLine(BitlapSqlCli()) with CliExecutor

  sealed trait CliExecutor {
    self: CommandLine =>

    /**
     * execute with args and return status code
     */
    def <<?[T: ClassTag](args: T): Int = {
      self.parseArgs()
      val clazz = classTag[T].runtimeClass
      if (clazz.isArray) {
        self.execute(args.asInstanceOf[Array[Object]].map(_.toString.trim).filter(_.nonEmpty): _*)
      } else {
        self.execute(args.asInstanceOf[String].split(" ").map(_.trim).filter(_.nonEmpty): _*)
      }
    }

    /**
     * execute with args and return execute console output
     */
    def <<<?[I: ClassTag](args: I): String = {
      val input = classTag[I].runtimeClass
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      self.setOut(pw).setErr(pw)
      if (input.isArray) {
        self.setOut(pw).setErr(pw).execute(args.asInstanceOf[Array[Object]].map(_.toString.trim).filter(_.nonEmpty): _*)
      } else {
        self.setOut(pw).setErr(pw).execute(args.asInstanceOf[String].split(" ").map(_.trim).filter(_.nonEmpty): _*)
      }
      sw.toString
    }
  }
}
