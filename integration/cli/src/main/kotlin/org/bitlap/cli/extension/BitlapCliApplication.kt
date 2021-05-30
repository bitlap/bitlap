package org.bitlap.cli.extension

import org.bitlap.common.BitlapConf
import sqlline.Application
import sqlline.CommandHandler
import sqlline.PromptHandler
import sqlline.SqlLine

/**
 * Desc: Extension for Application
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/18
 */
class BitlapCliApplication : Application() {

    override fun getCommandHandlers(sqlLine: SqlLine): MutableCollection<CommandHandler> {
        return super.getCommandHandlers(sqlLine)
    }

    override fun getPromptHandler(sqlLine: SqlLine): PromptHandler {
        val prompt = conf.get().get(BitlapConf.PROJECT_NAME)!!
        return BitlapPromptHandler(sqlLine, prompt)
    }

    companion object {
        val conf = ThreadLocal<BitlapConf>()
    }
}
