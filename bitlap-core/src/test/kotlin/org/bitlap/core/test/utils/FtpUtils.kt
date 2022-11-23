package org.bitlap.core.test.utils

import org.apache.ftpserver.FtpServer
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.usermanager.impl.WritePermission
import org.bitlap.common.logger
import java.net.ServerSocket

/**
 * Mail: k.chen@nio.com
 * Created by IceMimosa
 * Date: 2022/11/23
 */
object FtpUtils {

    private val log = logger {  }

    fun start(home: String): Pair<FtpServer, Int> {
        val port = ServerSocket(0).use {
            it.localPort
        }
        log.info { "ftp port: $port, home: $home" }
        return start(port, home) to port
    }

    fun start(port: Int, home: String): FtpServer {
        val server = FtpServerFactory()
        val factory = ListenerFactory().apply {
            this.serverAddress = "127.0.0.1"
            this.port = port
        }
        server.addListener("default", factory.createListener())
        server.userManager.save(BaseUser().apply {
            this.name = "bitlap"
            this.password = "bitlap"
            this.homeDirectory = home
            this.authorities = listOf(WritePermission())
        })
        return server.createServer().apply { this.start() }
    }
}
