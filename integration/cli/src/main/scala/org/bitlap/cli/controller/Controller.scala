package org.bitlap.cli.controller

import org.bitlap.cli.CLIContext
import zio.RIO

trait Controller {
  val controller: Controller.Service[Any]
}

object Controller {

  trait Service[R] {
    def process(input: String, cliContext: CLIContext): RIO[R, CLIContext]
  }
}