package org.bitlap.common

import com.google.protobuf.GeneratedMessageV3

/**
 *
 * @author 梦境迷离
 * @since 2021/6/13
 * @version 1.0
 */
interface RpcServiceSupport {

    fun registerMessageInstances(
        service: List<Pair<String, GeneratedMessageV3>>,
        action: (Pair<String, GeneratedMessageV3>) -> Unit
    ) {
        service.forEach { action(it) }
    }

    companion object {
        fun registerResp(): List<Pair<String, GeneratedMessageV3>> {
            return listOf(
                Pair(
                    org.bitlap.common.proto.rpc.HelloRpcPB.Req::class.java.name,
                    org.bitlap.common.proto.rpc.HelloRpcPB.Res.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BOpenSession.BOpenSessionReq::class.java.name,
                    org.bitlap.common.proto.driver.BOpenSession.BOpenSessionResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BCloseSession.BCloseSessionReq::class.java.name,
                    org.bitlap.common.proto.driver.BCloseSession.BCloseSessionResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BExecuteStatement.BExecuteStatementReq::class.java.name,
                    org.bitlap.common.proto.driver.BExecuteStatement.BExecuteStatementResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BFetchResults.BFetchResultsReq::class.java.name,
                    org.bitlap.common.proto.driver.BFetchResults.BFetchResultsResp.getDefaultInstance()
                ),
            )
        }

        fun registerReq(): List<Pair<String, GeneratedMessageV3>> {
            return listOf(
                Pair(
                    org.bitlap.common.proto.rpc.HelloRpcPB.Req::class.java.name,
                    org.bitlap.common.proto.rpc.HelloRpcPB.Req.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BOpenSession.BOpenSessionReq::class.java.name,
                    org.bitlap.common.proto.driver.BOpenSession.BOpenSessionReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BCloseSession.BCloseSessionReq::class.java.name,
                    org.bitlap.common.proto.driver.BCloseSession.BCloseSessionReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BExecuteStatement.BExecuteStatementReq::class.java.name,
                    org.bitlap.common.proto.driver.BExecuteStatement.BExecuteStatementReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.common.proto.driver.BFetchResults.BFetchResultsReq::class.java.name,
                    org.bitlap.common.proto.driver.BFetchResults.BFetchResultsReq.getDefaultInstance()
                ),
            )
        }
    }
}
