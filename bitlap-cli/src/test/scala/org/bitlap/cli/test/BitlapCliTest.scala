/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli.test

import org.bitlap.cli._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

class BitlapCliTest extends AnyFlatSpec with Matchers {

  private val helpText =
    s"""Usage: bitlap [-hV] [COMMAND]
       |bitlap cli command.
       |  -h, --help      Show this help message and exit.
       |  -V, --version   Print version information and exit.
       |Commands:
       |  server  A bitlap subcommand for server.
       |  sql     A bitlap subcommand for sql.
       |""".stripMargin

  "test bitlap cli" should "ok" in {
    // empty
    BitlapExecutor <<<? (Array(
      ""
    )) shouldBe s"Missing required subcommand\n$helpText"
    BitlapExecutor <<<? ("") shouldBe s"Missing required subcommand\n$helpText"
    // -V, --version
    BitlapExecutor <<<? ("-V") should include("v")
    BitlapExecutor <<<? ("--version") should include("v")
    // -h, --help
    BitlapExecutor <<<? ("-h") shouldBe helpText
    BitlapExecutor <<<? ("--help") shouldBe helpText
  }
}
