package org.bitlap.server.core

import org.bitlap.network.core.Session
import org.bitlap.network.core.SessionFactory
import org.bitlap.network.core.SessionManager

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/4
 */
class BitlapSessionFactory : SessionFactory {

    override fun create(username: String, password: String, sessionConf: Map<String, String>, sessionManager: SessionManager): Session {
        return BitlapSession(username, password, sessionConf, sessionManager)
    }
}
