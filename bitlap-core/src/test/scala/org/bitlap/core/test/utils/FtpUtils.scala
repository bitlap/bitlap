/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test.utils

import java.net.ServerSocket
import java.util.Collections

import org.bitlap.core.extension.*

import org.apache.ftpserver.FtpServer
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.usermanager.impl.WritePermission
import org.slf4j.LoggerFactory

object FtpUtils {

  private val log = LoggerFactory.getLogger(FtpUtils.getClass)

  def start(home: String): (FtpServer, Int) = {
    val port = ServerSocket(0).use { ss =>
      ss.getLocalPort
    }
    log.info(s"ftp port: $port, home: $home")
    start(port, home) -> port
  }

  def start(port: Int, home: String): FtpServer = {
    val server = FtpServerFactory()
    val factory = ListenerFactory().also { f =>
      f.setServerAddress("127.0.0.1")
      f.setPort(port)
    }
    server.addListener("default", factory.createListener())
    server.getUserManager.save(
      BaseUser().also { u =>
        u.setName("bitlap")
        u.setPassword("bitlap")
        u.setHomeDirectory(home)
        u.setAuthorities(Collections.singletonList(WritePermission()))
      }
    )
    server.createServer().also { ss => ss.start() }
  }
}
