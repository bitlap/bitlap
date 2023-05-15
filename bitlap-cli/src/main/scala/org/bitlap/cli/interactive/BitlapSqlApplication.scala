/* Copyright (c) 2023 bitlap.org */
package org.bitlap.cli.interactive

import java.util.Collection as JCollection

import org.bitlap.cli.interactive.BitlapSqlLineProperty.BitlapPrompt

import sqlline.*

final class BitlapSqlApplication extends Application {

  override def getCommandHandlers(
    sqlLine: SqlLine
  ): JCollection[CommandHandler] =
    super.getCommandHandlers(sqlLine)

  override def getPromptHandler(sqlLine: SqlLine): PromptHandler = {
    val prompt = sqlLine.getOpts.get(BitlapPrompt)
    new BitlapSqlPromptHandler(sqlLine, prompt)
  }
}
