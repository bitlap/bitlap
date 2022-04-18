/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.alipay.sofa.jraft.{ RouteTable, Status }
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.InvokeCallback
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.common.BitlapConf
import org.bitlap.tools.apply

/**
 * rpc client to wrapper sofa client
 */
@apply
class RpcClient(uri: String, conf: BitlapConf) {

  private var configuration: Configuration = _
  private var route: RouteTable = _
  private var client: CliClientServiceImpl = _

  // bitlap configuration
  private val groupId = conf.get(BitlapConf.NODE_GROUP_ID)
  private val timeout = conf.get(BitlapConf.NODE_RPC_TIMEOUT)

  // init code
  {
    configuration = new Configuration
    configuration.parse(uri)
    route = RouteTable.getInstance()
    route.updateConfiguration(groupId, configuration)
    client = new CliClientServiceImpl
    val opts = new CliOptions
    opts.setMaxRetry(3)
    client.init(opts)
    this.refreshLeader()
  }

  def invokeSync[T](request: AnyRef, timeout: Long = timeout, leader: Boolean = true): T = {
    // TODO: support other nodes
    this.refreshLeader()
    val node = if (leader) route.selectLeader(groupId) else route.selectLeader(groupId)
    val resp = this.client.getRpcClient.invokeSync(node.getEndpoint, request, timeout)
    resp.asInstanceOf[T]
  }

  def invokeAsync[T](
    request: AnyRef,
    callback: InvokeCallback,
    timeout: Long = timeout,
    leader: Boolean = true
  ): Unit = {
    // TODO: support other nodes
    this.refreshLeader()
    val node = if (leader) route.selectLeader(groupId) else route.selectLeader(groupId)
    this.client.getRpcClient.invokeAsync(node.getEndpoint, request, callback, timeout)
  }

  def refreshLeader(): Status = route.refreshLeader(client, groupId, timeout.toInt)
}
