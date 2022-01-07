package org.bitlap.cli.test

import org.bitlap.cli._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

class BitlapSqlCliTest extends AnyFlatSpec with Matchers {

  private val helpText =
    s"""Usage: bitlap [-hV] [COMMAND]
       |bitlap cli command.
       |  -h, --help      Show this help message and exit.
       |  -V, --version   Print version information and exit.
       |Commands:
       |  server  A bitlap subcommand for server.
       |  sql     A bitlap subcommand for sql.
       |""".stripMargin

  "test bitlap sql cli" should "ok" in {
    val cli = BitlapSqlCli()
    // empty
    cli >> "" shouldBe s""
    // -V, --version
//    cli >> "-V" should include("v")
//    cli >> "--version" should include("v")
    // -h, --help
    cli >> "-h 'select 123 from T'" shouldBe helpText
    cli >> "--help" shouldBe helpText
  }

}
