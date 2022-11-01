/* Copyright (c) 2022 bitlap.org */
package org.bitlap.cli.interactive

import org.bitlap.common.utils.StringEx
import org.bitlap.tools.apply
import sqlline._
import scala.collection.mutable

@apply
class BitlapSqlPromptHandler(val line: SqlLine, val prompt: String) extends PromptHandler(line) {

  override def getDefaultPrompt(
    connectionIndex: Int,
    url: String,
    defaultPrompt: String
  ): String = {
    val sb     = new mutable.StringBuilder(this.prompt)
    val schema = sqlLine.getConnectionMetadata.getCurrentSchema
    if (!StringEx.nullOrBlank(schema)) {
      sb.append(s" ($schema)")
    }
    sb.append("> ").toString
  }
}
