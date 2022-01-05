package org.bitlap.cli.test

import org.bitlap.cli.BitlapCli
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import picocli.CommandLine

import scala.language.postfixOps

class BitlapCliTest extends AnyFlatSpec with Matchers {

  "test bitlap cli" should "ok" in {
    val cli = new CommandLine(new BitlapCli())

    var args = Array[String]("-t", "aaa")
//    BitlapCli.main(args)

    cli.execute(args: _*)
    val r = cli.getExecutionResult[Int]

    println(s"$r ...")
  }
}
