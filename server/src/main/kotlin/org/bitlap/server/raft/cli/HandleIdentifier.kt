package org.bitlap.server.raft.cli

import com.google.protobuf.ByteString
import org.bitlap.common.proto.driver.BHandleIdentifier
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Abstract descriptor
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class HandleIdentifier {
    private var publicId: UUID
    private var secretId: UUID

    constructor() {
        publicId = UUID.randomUUID()
        secretId = UUID.randomUUID()
    }

    constructor(publicId: UUID, secretId: UUID) : this() {
        this.publicId = publicId
        this.secretId = secretId
    }

    constructor(bHandleId: BHandleIdentifier) : this() {
        var bb = ByteBuffer.wrap(ByteString.copyFromUtf8(bHandleId.guid).toByteArray())
        publicId = UUID(bb.long, bb.long)
        bb = ByteBuffer.wrap(ByteString.copyFromUtf8(bHandleId.secret).toByteArray())
        secretId = UUID(bb.long, bb.long)
    }

    fun getPublicId(): UUID = publicId

    fun getSecretId(): UUID = secretId

    open fun toBHandleIdentifier(): BHandleIdentifier {
        val guid = ByteArray(16)
        val secret = ByteArray(16)
        val guidBB = ByteBuffer.wrap(guid)
        val secretBB = ByteBuffer.wrap(secret)
        guidBB.putLong(publicId.mostSignificantBits)
        guidBB.putLong(publicId.leastSignificantBits)
        secretBB.putLong(secretId.mostSignificantBits)
        secretBB.putLong(secretId.leastSignificantBits)
        return BHandleIdentifier.newBuilder().setGuid(ByteString.copyFrom(guid).toStringUtf8())
            .setSecret(ByteString.copyFrom(secret).toStringUtf8()).build()
    }

    override fun hashCode(): Int {
        val prime = 31
        var result = 1
        result = prime * result + publicId.hashCode()
        result = prime * result + secretId.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null) {
            return false
        }
        if (other !is HandleIdentifier) {
            return false
        }
        if (publicId != other.publicId) {
            return false
        }
        if (secretId != other.secretId) {
            return false
        }
        return true
    }

    override fun toString(): String = publicId.toString()
}
