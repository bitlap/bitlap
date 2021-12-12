package org.bitlap.cli

import zio.ZIO
import zio.macros.annotation.accessible

import java.io.IOException

/**
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
@accessible(">")
trait Terminal {

  val terminal: Terminal.Service[Any]

}

object Terminal {

  trait Service[R] {

    val getUserInput: ZIO[R, IOException, String]

    def display(cliContext: CLIContext): ZIO[R, IOException, Unit]
  }
}