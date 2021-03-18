package org.bitlap.core

import org.bitlap.core.model.query.Query
import java.io.Closeable

/**
 * Desc: Bitlap reader to handle data
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/03/17
 */
interface BitlapReader : Closeable {

    fun read(query: Query)
}
