package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor
import com.google.protobuf.Message
import org.bitlap.common.logger
import java.util.concurrent.Executor

/**
 * Desc: common abstract for rpc processors
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/4
 */
abstract class BitlapRpcProcessor<T : Message>(
    executor: Executor?,
    private val defaultResp: Message,
) : RpcRequestProcessor<T>(executor, defaultResp), ProcessorHelper {

    val log = logger { }

    override fun handleRequest(rpcCtx: RpcContext, request: T) {
        try {
            val msg = processRequest(request, RpcRequestClosure(rpcCtx, this.defaultResp))
            if (msg != null) {
                rpcCtx.sendResponse(msg)
            }
        } catch (e: Exception) {
            log.error("handleRequest $request failed", e)
            rpcCtx.sendResponse(processError(rpcCtx, e))
        }
    }

    abstract fun processError(rpcCtx: RpcContext, exception: Exception): Message
}
