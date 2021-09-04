package org.bitlap.jdbc

import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.network.BSQLException
import org.bitlap.network.NetworkHelper
import org.bitlap.network.client.BitlapClient
import org.bitlap.network.client.BitlapClient.closeSession
import org.bitlap.network.client.BitlapClient.openSession
import org.bitlap.network.proto.driver.BSessionHandle
import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.NClob
import java.sql.PreparedStatement
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.Statement
import java.sql.Struct
import java.util.Properties
import java.util.concurrent.Executor

/**
 * Bitlap Connection
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class BitlapConnection(uri: String, info: Properties = Properties()) : Connection, NetworkHelper {

    companion object {
        private const val URI_PREFIX = "jdbc:bitlap://"
    }

    private var session: BSessionHandle? = null
    // TODO Consider replace it
    private val client: CliClientServiceImpl by lazy { CliClientServiceImpl() }
    private var isClosed = true
    private var warningChain: SQLWarning? = null

    init {
        BitlapClient.beforeInit()

        if (!uri.startsWith(URI_PREFIX)) {
            throw Exception("Invalid URL: $uri")
        }
        // remove prefix
        val uriWithoutPrefix = uri.substring(URI_PREFIX.length)
        val hosts = uriWithoutPrefix.split(",")
        // parse uri
        val parts = hosts.map { it.split("/").toTypedArray() }
        try {
            val conf = Configuration()
            parts.forEach { conf.parse(it[0]) } // Not tested
            session = client.openSession(conf, info)
            isClosed = false
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun close() {
        try {
            session?.let { client.closeSession(it) }
        } finally {
            isClosed = true
        }
    }

    override fun createStatement(): Statement {
        if (session != null) {
            return BitlapStatement(session!!, client)
        } else {
            throw BSQLException("Statement is closed")
        }
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        TODO("Not yet implemented")
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareCall(sql: String?): CallableStatement {
        TODO("Not yet implemented")
    }

    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int): CallableStatement {
        TODO("Not yet implemented")
    }

    override fun prepareCall(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement {
        TODO("Not yet implemented")
    }

    override fun nativeSQL(sql: String?): String {
        TODO("Not yet implemented")
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        TODO("Not yet implemented")
    }

    override fun getAutoCommit(): Boolean {
        TODO("Not yet implemented")
    }

    override fun commit() {
        TODO("Not yet implemented")
    }

    override fun rollback() {
        TODO("Not yet implemented")
    }

    override fun rollback(savepoint: Savepoint?) {
        TODO("Not yet implemented")
    }

    override fun isClosed(): Boolean {
        return isClosed
    }

    override fun getMetaData(): DatabaseMetaData {
        TODO("Not yet implemented")
    }

    override fun setReadOnly(readOnly: Boolean) {
        TODO("Not yet implemented")
    }

    override fun isReadOnly(): Boolean {
        TODO("Not yet implemented")
    }

    override fun setCatalog(catalog: String?) {
        TODO("Not yet implemented")
    }

    override fun getCatalog(): String {
        TODO("Not yet implemented")
    }

    override fun setTransactionIsolation(level: Int) {
        TODO("Not yet implemented")
    }

    override fun getTransactionIsolation(): Int {
        TODO("Not yet implemented")
    }

    override fun getWarnings(): SQLWarning {
        TODO("Not yet implemented")
    }

    override fun clearWarnings() {
        warningChain = null
    }

    override fun getTypeMap(): MutableMap<String, Class<*>> {
        TODO("Not yet implemented")
    }

    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {
        TODO("Not yet implemented")
    }

    override fun setHoldability(holdability: Int) {
        TODO("Not yet implemented")
    }

    override fun getHoldability(): Int {
        TODO("Not yet implemented")
    }

    override fun setSavepoint(): Savepoint {
        TODO("Not yet implemented")
    }

    override fun setSavepoint(name: String?): Savepoint {
        TODO("Not yet implemented")
    }

    override fun releaseSavepoint(savepoint: Savepoint?) {
        TODO("Not yet implemented")
    }

    override fun createClob(): Clob {
        TODO("Not yet implemented")
    }

    override fun createBlob(): Blob {
        TODO("Not yet implemented")
    }

    override fun createNClob(): NClob {
        TODO("Not yet implemented")
    }

    override fun createSQLXML(): SQLXML {
        TODO("Not yet implemented")
    }

    override fun isValid(timeout: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun setClientInfo(name: String?, value: String?) {
        TODO("Not yet implemented")
    }

    override fun setClientInfo(properties: Properties?) {
        TODO("Not yet implemented")
    }

    override fun getClientInfo(name: String?): String {
        TODO("Not yet implemented")
    }

    override fun getClientInfo(): Properties {
        TODO("Not yet implemented")
    }

    override fun createArrayOf(typeName: String?, elements: Array<out Any>?): java.sql.Array {
        TODO("Not yet implemented")
    }

    override fun createStruct(typeName: String?, attributes: Array<out Any>?): Struct {
        TODO("Not yet implemented")
    }

    override fun setSchema(schema: String?) {
        TODO("Not yet implemented")
    }

    override fun getSchema(): String {
        TODO("Not yet implemented")
    }

    override fun abort(executor: Executor?) {
        TODO("Not yet implemented")
    }

    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {
        TODO("Not yet implemented")
    }

    override fun getNetworkTimeout(): Int {
        TODO("Not yet implemented")
    }
}
