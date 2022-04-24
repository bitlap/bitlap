/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli.interactive

import org.bitlap.tools.apply
import sqlline.{ Application, CommandHandler, PromptHandler, SqlLine }

import java.util

@apply
class BitlapSqlApplication extends Application {

  override def getCommandHandlers(
    sqlLine: SqlLine
  ): util.Collection[CommandHandler] =
    super.getCommandHandlers(sqlLine)

  override def getPromptHandler(sqlLine: SqlLine): PromptHandler = {
    val prompt = sqlLine.getOpts.get(BitlapSqlLineDefaultProperty)
    BitlapSqlPromptHandler(sqlLine, prompt)
  }
}
