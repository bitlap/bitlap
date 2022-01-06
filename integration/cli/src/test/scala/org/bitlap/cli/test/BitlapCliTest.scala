package org.bitlap.cli.test

import org.bitlap.cli.{BitlapCli, _}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

class BitlapCliTest extends AnyFlatSpec with Matchers {

  "test bitlap cli" should "ok" in {
    val cli = BitlapCli()
    // empty
    cli >> "" shouldBe s"Missing required subcommand\n$helpText"
    // -V, --version
    cli >> "-V" should include("v")
    cli >> "--version" should include("v")
    // -h, --help
    cli >> "-h" shouldBe helpText
    cli >> "--help" shouldBe helpText
  }

  private val helpText =
    s"""Usage: bitlap [-hV] [COMMAND]
       |bitlap cli command.
       |  -h, --help      Show this help message and exit.
       |  -V, --version   Print version information and exit.
       |Commands:
       |  server  A bitlap subcommand for server.
       |  sql     A bitlap subcommand for sql.
       |""".stripMargin
}
