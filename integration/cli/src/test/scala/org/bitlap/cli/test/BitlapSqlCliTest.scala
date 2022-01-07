package org.bitlap.cli.test

import org.bitlap.cli._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

class BitlapSqlCliTest extends AnyFlatSpec with Matchers {

  private val helpText = """Usage: sql [-hV] [-p] [-s=SERVER] [-u=USERNAME] [SQL...]
                           |A bitlap subcommand for sql.
                           |      [SQL...]          SQL to execute.
                           |  -h, --help            Show this help message and exit.
                           |  -p, --password        User password.
                           |  -s, --server=SERVER   Server Address.
                           |  -u, --user=USERNAME   User name.
                           |  -V, --version         Print version information and exit.
                           |""".stripMargin

  "test bitlap sql cli" should "ok" in {
    // empty
    BitlapSqlExecutor <<<? "" shouldBe s""
    BitlapSqlExecutor <<<? "-h 'select 123 from T'" shouldBe helpText
    BitlapSqlExecutor <<<? "--help" shouldBe helpText
  }

}
