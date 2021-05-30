package org.bitlap.cli.cmd

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/25
 */
class BitlapCommandParser(val cmd: String, val args: Array<String> = emptyArray()) {
    private val parser = ArgParser(this.cmd)

    internal val server by parser.option(ArgType.String, shortName = "s", description = "Bitlap server address.").required() // TODO .default()

    fun execute(): BitlapCommandParser {
        this.parser.parse(args)
        return this
    }
}
