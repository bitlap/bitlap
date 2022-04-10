/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import org.bitlap.common.BitlapConf

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
object BitlapServer extends App {
  val conf = new BitlapConf()
  val server = new BitlapServerEndpoint(conf)
  Runtime.getRuntime.addShutdownHook(
    new Thread {
      if (server != null && !server.isShutdown) {
        server.close()
      }
    }
  )
  this.server.start()
  this.server.join()
}
