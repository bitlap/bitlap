package org.bitlap.server.raft.cli.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BGetResultSetMetadata
import org.bitlap.server.raft.cli.CLIService

/**
 * GetResultSetMetadata
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetResultSetMetaDataProcessor(private val cliService: CLIService) :
    RpcProcessor<BGetResultSetMetadata.BGetResultSetMetadataReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetResultSetMetadata.BGetResultSetMetadataReq) {
    }

    override fun interest(): String = BGetResultSetMetadata.BGetResultSetMetadataReq::class.java.name
}
