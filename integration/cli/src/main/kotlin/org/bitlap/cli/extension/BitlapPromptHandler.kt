package org.bitlap.cli.extension

import sqlline.PromptHandler
import sqlline.SqlLine

/**
 * Desc: Extension for PromptHandler
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/18
 */
class BitlapPromptHandler(sqlline: SqlLine, prompt: String) : PromptHandler(sqlline) {

    override fun getDefaultPrompt(connectionIndex: Int, url: String?, defaultPrompt: String?): String {
        return StringBuilder(prompt).apply {
            sqlLine.connectionMetadata.currentSchema?.also {
                append(" ($it)")
            }
            append("> ")
        }.toString()
    }
}
