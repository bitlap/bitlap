package org.bitlap.common.utils

import java.io.File

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/30
 */
object StringEx {

    /**
     * Fix path, concat [subPaths] with [File.pathSeparator]
     */
    fun String.withPaths(vararg subPaths: String): String {
        val separator = File.separator
        return subPaths.fold(this) { p1, p2 ->
            when {
                p2.isBlank() -> p1
                p1.endsWith(separator) && p2.startsWith(separator) -> p1 + p2.substring(1)
                p1.endsWith(separator) || p2.startsWith(separator) -> p1 + p2
                else -> p1 + separator + p2
            }
        }
    }
}
