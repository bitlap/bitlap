package org.bitlap.cli

import zio.RIO
import zio.macros.annotation.accessible

@accessible(">")
trait RunSQL {

  val runSQL: RunSQL.Service[Any]
}

object RunSQL {

  trait Service[R] {

    def exec(cliContext: CLIContext): RIO[R, CLIContext]
  }
}