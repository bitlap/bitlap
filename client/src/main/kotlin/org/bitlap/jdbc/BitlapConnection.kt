package org.bitlap.jdbc

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import com.google.protobuf.ByteString
import org.bitlap.common.BitlapConf
import org.bitlap.common.proto.driver.BCloseSession
import org.bitlap.common.proto.driver.BExecuteStatement
import org.bitlap.common.proto.driver.BFetchResults
import org.bitlap.common.proto.driver.BOpenSession
import org.bitlap.common.proto.rpc.HelloRpcPB
import java.nio.ByteBuffer
import java.sql.*
import java.util.Properties
import java.util.UUID
import java.util.concurrent.Executor


/**
 * Bitlap Connection
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class BitlapConnection(private var uri: String, val info: Properties?) : Connection {

    companion object {
        private const val URI_PREFIX = "jdbc:bitlap://"

    }

    private var session: JdbcSessionState
    private var client: CliClientServiceImpl = CliClientServiceImpl()
    private var isClosed = true
    private var warningChain: SQLWarning? = null

    init {
        listOf(
            Pair(HelloRpcPB.Req::class.java.name, HelloRpcPB.Res.getDefaultInstance()),
            Pair(BOpenSession.BOpenSessionReq::class.java.name, BOpenSession.BOpenSessionReq.getDefaultInstance()),
            Pair(BCloseSession.BCloseSessionReq::class.java.name, BCloseSession.BCloseSessionReq.getDefaultInstance()),
            Pair(
                BExecuteStatement.BExecuteStatementReq::class.java.name,
                BExecuteStatement.BExecuteStatementReq.getDefaultInstance()
            ),
            Pair(BFetchResults.BFetchResultsReq::class.java.name, BFetchResults.BFetchResultsReq.getDefaultInstance()),
        ).forEach {
            RpcFactoryHelper.rpcFactory()
                .registerProtobufSerializer(it.first, it.second)
        }

        listOf(
            Pair(HelloRpcPB.Req::class.java.name, HelloRpcPB.Req.getDefaultInstance()),
            Pair(BOpenSession.BOpenSessionReq::class.java.name, BOpenSession.BOpenSessionResp.getDefaultInstance()),
            Pair(BCloseSession.BCloseSessionReq::class.java.name, BCloseSession.BCloseSessionResp.getDefaultInstance()),
            Pair(
                BExecuteStatement.BExecuteStatementReq::class.java.name,
                BExecuteStatement.BExecuteStatementResp.getDefaultInstance()
            ),
            Pair(BFetchResults.BFetchResultsReq::class.java.name, BFetchResults.BFetchResultsResp.getDefaultInstance()),
        ).forEach {
            MarshallerHelper.registerRespInstance(it.first, it.second)
        }

        session = JdbcSessionState(BitlapConf())
        JdbcSessionState.start(session)
        if (!uri.startsWith(URI_PREFIX)) {
            throw Exception("Invalid URL: $uri")
        }
        // remove prefix
        uri = uri.substring(URI_PREFIX.length)
        // parse uri
        val parts = uri.split("/").toTypedArray()
        try {
            // TODO Secondary wrap for registration and release
            val groupId = "bitlap-cluster"
            val conf = Configuration()
            conf.parse(parts[0])
            RouteTable.getInstance().updateConfiguration(groupId, conf)
            client.init(CliOptions())
            check(RouteTable.getInstance().refreshLeader(client, groupId, 1000).isOk) { "Refresh leader failed" }
            val leader = RouteTable.getInstance().selectLeader(groupId)
            println("Leader: $leader")
            client.rpcClient.invokeAsync(
                leader.endpoint, BOpenSession.BOpenSessionReq.newBuilder().setUsername(info?.get("user").toString())
                    .setPassword(info?.get("password").toString()).build(), { result, err ->
                    result as BOpenSession.BOpenSessionResp
                    val id = ByteBuffer.wrap(ByteString.copyFromUtf8(result.sessionHandle.sessionId.guid).toByteArray())
                    println("Open session: ${UUID(id.long, id.long)}")

                }, 5000
            )
            Thread.currentThread().join()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun close() {
//        try {
//            val groupId = "bitlap-cluster"
//            val leader = RouteTable.getInstance().selectLeader(groupId)
//            if (cli.isConnected(leader.endpoint)) cli.shutdown()
//        } finally {
//            isClosed = true
//        }
    }

    override fun createStatement(): Statement {
        return BitlapStatement(session, client)
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareStatement(sql: String): PreparedStatement {
        return BitlapPreparedStatement(sql)
    }

    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareStatement(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareCall(sql: String?): CallableStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int): CallableStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun prepareCall(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun nativeSQL(sql: String?): String {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getAutoCommit(): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun commit() {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun rollback() {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun rollback(savepoint: Savepoint?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun isClosed(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getMetaData(): DatabaseMetaData {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setReadOnly(readOnly: Boolean) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun isReadOnly(): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setCatalog(catalog: String?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getCatalog(): String {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setTransactionIsolation(level: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getTransactionIsolation(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getWarnings(): SQLWarning {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun clearWarnings() {
        warningChain = null
    }

    override fun getTypeMap(): MutableMap<String, Class<*>> {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setHoldability(holdability: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getHoldability(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setSavepoint(): Savepoint {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setSavepoint(name: String?): Savepoint {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun releaseSavepoint(savepoint: Savepoint?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun createClob(): Clob {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun createBlob(): Blob {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun createNClob(): NClob {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun createSQLXML(): SQLXML {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun isValid(timeout: Int): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setClientInfo(name: String?, value: String?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setClientInfo(properties: Properties?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getClientInfo(name: String?): String {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getClientInfo(): Properties {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun createArrayOf(typeName: String?, elements: Array<out Any>?): java.sql.Array {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun createStruct(typeName: String?, attributes: Array<out Any>?): Struct {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setSchema(schema: String?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getSchema(): String {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun abort(executor: Executor?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    override fun getNetworkTimeout(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }
}
