package org.bitlap.cli

import org.bitlap.cli.extension.BitlapCliApplication
import sqlline.SqlLine
import sqlline.SqlLineOpts
import java.io.File
import java.util.Locale
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
        val baseDir = File(
            System.getProperty("user.home"),
            (
                if (System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")) ""
                else "."
                ) + "bitlap"
        ).absolutePath
        System.setProperty("x.sqlline.basedir", baseDir)
        val sqlline = SqlLine().apply {
//            opts.set(BuiltInProperty.PROMPT, "bitlap> ")
//            updateCommandHandlers()
        }
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
