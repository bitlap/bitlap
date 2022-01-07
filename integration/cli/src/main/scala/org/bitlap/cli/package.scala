package org.bitlap

import picocli.CommandLine

import java.io.{PrintWriter, StringWriter}
import scala.language.implicitConversions

package object cli {

  trait Cli extends Runnable {
    override def run(): Unit = {}
  }

  implicit def cmd(cli: Cli): CommandLine = new CommandLine(cli)

  implicit class CommandLineWrapper(val cli: Cli) {

    // execute cmd with input args
    def >(str: String): Int = {
      val args = str.split(" ").map(_.trim).filter(_.nonEmpty)
      this.cli > args
    }

    def >(args: Array[String]): Int = {
      this.cli.execute(args: _*)
    }

    // execute cmd with input args, return execute output
    def >>(str: String): String = {
      val args = str.split(" ").map(_.trim).filter(_.nonEmpty)
      this.cli >> args
    }

    def >>(args: Array[String]): String = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      this.cli.setOut(pw).setErr(pw).execute(args: _*)
      sw.toString
    }
  }
}
