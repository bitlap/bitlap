/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli.interactive

import org.bitlap.cli.interactive.BitlapSqlLineProperty.BitlapPrompt
import org.bitlap.tools.apply
import sqlline._

import java.util.{ Collection => JCollection }

@apply
class BitlapSqlApplication extends Application {

  override def getCommandHandlers(
    sqlLine: SqlLine
  ): JCollection[CommandHandler] =
    super.getCommandHandlers(sqlLine)

  override def getPromptHandler(sqlLine: SqlLine): PromptHandler = {
    val prompt = sqlLine.getOpts.get(BitlapPrompt)
    BitlapSqlPromptHandler(sqlLine, prompt)
  }
}
