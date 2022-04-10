/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.alipay.sofa.jraft.{ Node, RaftGroupService }
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions
import com.alipay.sofa.jraft.rpc.{ RaftRpcServerFactory, RpcRequestProcessor }
import org.bitlap.common.BitlapConf

class RpcServer(val conf: BitlapConf, val processors: List[RpcRequestProcessor[_]]) {

  // bitlap configuration
  private val groupId = conf.get(BitlapConf.NODE_GROUP_ID)
  private val host = conf.get(BitlapConf.NODE_BIND_HOST)

  // server
  private val hostId = PeerId.parsePeer(host)
  private val server = RaftRpcServerFactory.createRaftRpcServer(hostId.getEndpoint)
  processors.foreach(server.registerProcessor)

  def start(options: NodeOptions): Node = {
    val raftGroupService = new RaftGroupService(groupId, hostId, options, server)
    raftGroupService.start()
  }
}
