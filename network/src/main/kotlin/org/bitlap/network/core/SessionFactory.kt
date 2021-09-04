package org.bitlap.network.core

/**
 * Desc: create session impl from spi
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/4
 */
interface SessionFactory {

    fun create(username: String, password: String, sessionConf: Map<String, String>, sessionManager: SessionManager): Session
}
