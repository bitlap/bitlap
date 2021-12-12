package org.bitlap.cli.run

import org.bitlap.cli.CLIContext
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