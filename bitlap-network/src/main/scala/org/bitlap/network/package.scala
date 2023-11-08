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
package org.bitlap.network

import org.bitlap.common.exception.*
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.ServerAddress

import com.typesafe.scalalogging.LazyLogging

import io.grpc.*

type Identity[T] = T

private[bitlap] final case class ServerAddress(ip: String, port: Int)

private val Separator = ":"
private val Port      = 23333

/** Parsing IP:PORT from String, returning [[org.bitlap.network.ServerAddress]].
 */
extension (serverUri: String)

  def asServerAddress: ServerAddress = {
    val as =
      if serverUri.contains(Separator) then serverUri.split(Separator).toList
      else List(serverUri, Port.toString)
    ServerAddress(as.head.trim, as(1).trim.toIntOption.getOrElse(Port))
  }
end extension

/** Parsing Array(IP:PORT,IP:PORT,IP:PORT,...) from String, returning a list of the
 *  [[org.bitlap.network.ServerAddress]].
 */
extension (serverPeers: Array[String])

  def asServerAddresses: List[ServerAddress] =
    serverPeers.collect {
      case add if add.nonEmpty => add.asServerAddress
    }.toList
  end asServerAddresses

end extension

final case class ClientConfig(
  props: Map[String, String],
  serverPeers: List[String])

lazy val errorApplyFunc: Throwable => StatusException = {
  case net @ BitlapException(errorKey, parameters, cause) =>
    new StatusException(Status.fromThrowable(net).withDescription(errorKey.formatErrorMessage(parameters)))
  case ex => new StatusException(Status.fromThrowable(ex))
}
