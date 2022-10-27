/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.ariskk.raft.model.{ Command, Serde }
import com.google.protobuf.ByteString
import io.grpc.{ ManagedChannelBuilder, Status }
import org.bitlap.network.raft.{ CommandType, RaftCommandReq }
import org.bitlap.network.NetworkException.RpcException
import org.bitlap.network.raft.ZioRaft.RaftServiceClient
import zio.{ Layer, Schedule, ZIO }
import zio.clock.Clock
import zio.duration.Duration

import java.util.concurrent.TimeUnit
import scala.util.Try

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/27
 */
trait Invoker {

  def rpcClient(
    msg: ByteString,
    address: String,
    port: Int
  ): ZIO[Any, RpcException, Array[Byte]] = {
    val client: Layer[Throwable, RaftServiceClient] = RaftServiceClient.live(
      scalapb.zio_grpc
        .ZManagedChannel(builder =
          ManagedChannelBuilder.forAddress(address, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]]
        )
    )
    (for {
      chunks <- RaftServiceClient
        .submit(
          RaftCommandReq(
            msg,
            CommandType.COMMAND
          )
        )
        .provideLayer(client)
        .mapError(e => RpcException(msg = "rpc error", cause = Try(e.asInstanceOf[Status].asException()).toOption))
    } yield chunks.data.toByteArray)
  }

}
