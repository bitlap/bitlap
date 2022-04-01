/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli.extension

import org.bitlap.cli.extension.BitlapSqlApplication.conf
import org.bitlap.common.BitlapConf
import org.bitlap.tools.apply
import sqlline.{ Application, CommandHandler, PromptHandler, SqlLine }

import java.util

@apply
class BitlapSqlApplication extends Application {

  override def getCommandHandlers(sqlLine: SqlLine): util.Collection[CommandHandler] =
    super.getCommandHandlers(sqlLine)

  override def getPromptHandler(sqlLine: SqlLine): PromptHandler = {
    val prompt = conf.get().get(BitlapConf.PROJECT_NAME)
    BitlapSqlPromptHandler(sqlLine, prompt)
  }
}

object BitlapSqlApplication {
  val conf = new ThreadLocal[BitlapConf]()
}
