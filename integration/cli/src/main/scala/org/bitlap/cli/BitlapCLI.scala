package org.bitlap.cli

import zio.console.Console
import zio.{ App, CanFail, UIO, ZIO, console }

import java.io.IOException

/**
 * cli客户端 基于ZIO
 *
 * @author 梦境迷离
 * @since 2021/12/12
 * @version 1.0
 */
object BitlapCLI extends App {

  val program: ZIO[RunSQL, Nothing, Unit] = {
    def exec(sqlContext: CLIContext): ZIO[RunSQL, Nothing, Unit] = {
      RunSQL.>.exec(sqlContext).foldM(
        _ => UIO.unit,
        newSqlContext => exec(newSqlContext)
      )
    }

    exec(CLIContext.apply(""))
  }

  def run(args: List[String]) =
    for {
      env <- prepareEnvironment
      out <- program.provide(env).foldM(
        error => console.putStrLn(s"Execution failed with: $error").exitCode
        , _ => UIO.succeed(0)
      )(CanFail.canFail[IOException]).exitCode
    } yield out

  private val prepareEnvironment =
    UIO.succeed(
      new ControllerLive
        with TerminalLive
        with RunSQLLive
        with CLICommandParserLive {
        override val console = Console.Service.live
      }
    )
}
