package org.bitlap.network.core

import com.google.protobuf.ByteString
import org.bitlap.network.proto.driver.BHandleIdentifier
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Unique ID implementation for session id and operation id.
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class HandleIdentifier(
    var publicId: UUID = UUID.randomUUID(),
    var secretId: UUID = UUID.randomUUID()
) {

    constructor(bHandleId: BHandleIdentifier) : this() {
        var bb = ByteBuffer.wrap(bHandleId.guid.toByteArray())
        this.publicId = UUID(bb.long, bb.long)
        bb = ByteBuffer.wrap(bHandleId.secret.toByteArray())
        this.secretId = UUID(bb.long, bb.long)
    }

    fun toBHandleIdentifier(): BHandleIdentifier {
        val guid = ByteArray(16)
        val secret = ByteArray(16)
        val guidBB = ByteBuffer.wrap(guid)
        val secretBB = ByteBuffer.wrap(secret)
        guidBB.putLong(this.publicId.mostSignificantBits)
        guidBB.putLong(this.publicId.leastSignificantBits)
        secretBB.putLong(this.secretId.mostSignificantBits)
        secretBB.putLong(this.secretId.leastSignificantBits)
        return BHandleIdentifier.newBuilder().setGuid(ByteString.copyFrom(guid))
            .setSecret(ByteString.copyFrom(secret)).build()
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
