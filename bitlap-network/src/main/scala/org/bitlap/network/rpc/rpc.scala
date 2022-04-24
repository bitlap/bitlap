/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
package object rpc {

  type Identity[X] = X

  lazy val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.global

}
