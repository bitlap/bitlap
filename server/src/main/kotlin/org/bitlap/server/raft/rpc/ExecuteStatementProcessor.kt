package org.bitlap.server.raft.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BExecuteStatement

/**
 * ExecuteStatement
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class ExecuteStatementProcessor : RpcProcessor<BExecuteStatement.BExecuteStatementReq> {
    override fun handleRequest(rpcCtx: RpcContext, request: BExecuteStatement.BExecuteStatementReq) {
        TODO("Not yet implemented")
    }

    override fun interest(): String = BExecuteStatement.BExecuteStatementReq::class.java.name
}
