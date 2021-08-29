package org.bitlap.cli

import org.bitlap.cli.cmd.BitlapCommandParser
import org.bitlap.cli.extension.BitlapCliApplication
import org.bitlap.common.BitlapConf
import sqlline.SqlLine
import sqlline.SqlLineOpts
import java.io.File
import kotlin.system.exitProcess

/**
 * Desc: Bitlap sql line
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/18
 */
object BitlapSqlLine {

    @JvmStatic
    fun main(args: Array<String>) {
        // TODO: 引入 https://github.com/ajalt/mordant

        val conf = BitlapConf()
        val projectName = conf.get(BitlapConf.PROJECT_NAME)!!
        val parser = BitlapCommandParser(projectName, arrayOf()).execute()

        val baseDir = File(
            System.getProperty("user.home"),
            (
                if (System.getProperty("os.name").lowercase().contains("windows")) ""
                else "."
                ) + projectName
        ).absolutePath
        System.setProperty("x.sqlline.basedir", baseDir)
        val sqlline = SqlLine().apply {
//            opts.set(BuiltInProperty.PROMPT, "bitlap> ")
//            updateCommandHandlers()
        }
        BitlapCliApplication.conf.set(conf)
        val status = sqlline.begin(
            arrayOf(
                "-d", "org.h2.Driver",
                "-u", "jdbc:h2:mem:",
                "-n", "sa",
                "-p", "",
                "-ac", BitlapCliApplication::class.java.canonicalName
            ),
            null, false
        )
        if (!SqlLineOpts.PROPERTY_NAME_EXIT.toBoolean()) {
            exitProcess(status.ordinal)
        }
    }
}
