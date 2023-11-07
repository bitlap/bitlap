/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.test.utils

import java.net.ServerSocket
import java.util.Collections

import org.bitlap.common.extension._

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
