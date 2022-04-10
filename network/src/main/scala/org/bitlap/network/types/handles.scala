package org.bitlap.network.types

import com.google.protobuf.ByteString
import OperationType.OperationType
import org.bitlap.network.proto.driver.{ BHandleIdentifier, BOperationHandle, BOperationType, BSessionHandle }
import org.bitlap.tools.toString

import java.nio.ByteBuffer
import java.util.UUID

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
object handles {

  /**
   * 抽象处理器
   *
   * @param handleId
   */
  abstract class Handle(
    val handleId: HandleIdentifier = new HandleIdentifier()
  ) {

    // super不能直接引用handleId属性
    def getHandleId(): HandleIdentifier = handleId

    def this(bHandleIdentifier: BHandleIdentifier) = {
      this(new HandleIdentifier(bHandleIdentifier))
    }

    override def hashCode(): Int = {
      val prime = 31
      var result = 1
      result = prime * result + handleId.hashCode()
      result
    }

    override def equals(other: Any): Boolean = {
      if (this.eq(other.asInstanceOf[AnyRef])) {
        return true
      }
      if (other == null) {
        return false
      }
      if (!other.isInstanceOf[Handle]) {
        return false
      }
      val otherHandle = other.asInstanceOf[Handle]
      if (handleId != otherHandle.handleId) {
        return false
      }
      true
    }

    def toString: String
  }

  /**
   * 统一标识符定义
   *
   * @param publicId
   * @param secretId
   */
  @toString(includeFieldNames = true)
  class HandleIdentifier(
    var publicId: UUID = UUID.randomUUID(),
    var secretId: UUID = UUID.randomUUID()
  ) {

    def this(bHandleId: BHandleIdentifier) = {
      this()
      var bb = ByteBuffer.wrap(bHandleId.getGuid.toByteArray)
      this.publicId = new UUID(bb.getLong, bb.getLong)
      bb = ByteBuffer.wrap(bHandleId.getSecret.toByteArray)
      this.secretId = new UUID(bb.getLong, bb.getLong)
    }

    def toBHandleIdentifier(): BHandleIdentifier = {
      val guid = new Array[Byte](16)
      val secret = new Array[Byte](16)
      val guidBB = ByteBuffer.wrap(guid)
      val secretBB = ByteBuffer.wrap(secret)
      guidBB.putLong(this.publicId.getMostSignificantBits)
      guidBB.putLong(this.publicId.getLeastSignificantBits)
      secretBB.putLong(this.secretId.getMostSignificantBits)
      secretBB.putLong(this.secretId.getLeastSignificantBits)
      BHandleIdentifier
        .newBuilder()
        .setGuid(ByteString.copyFrom(guid))
        .setSecret(ByteString.copyFrom(secret))
        .build()
    }

    // 理论上hashCode和equals也可以用，@equalsAndHashCode
    override def hashCode(): Int = {
      val prime = 31
      var result = 1
      result = prime * result + publicId.hashCode()
      result = prime * result + secretId.hashCode()
      result
    }

    override def equals(other: Any): Boolean = {
      if (this.eq(other.asInstanceOf[AnyRef])) {
        return true
      }
      if (other == null) {
        return false
      }
      if (!other.isInstanceOf[HandleIdentifier]) {
        return false
      }
      val o = other.asInstanceOf[HandleIdentifier]
      if (publicId != o.publicId) {
        return false
      }
      if (secretId != o.secretId) {
        return false
      }
      true
    }
  }

  /**
   * 会话处理器句柄
   *
   * @param handleId
   */
  @toString(includeFieldNames = true)
  class SessionHandle(override val handleId: HandleIdentifier) extends Handle(handleId) {

    def this(bSessionHandle: BSessionHandle) =
      this(new HandleIdentifier(bSessionHandle.getSessionId))

    def toBSessionHandle(): BSessionHandle =
      BSessionHandle
        .newBuilder()
        .setSessionId(super.getHandleId().toBHandleIdentifier())
        .build()

    override def equals(other: Any): Boolean = {
      if (this.eq(other.asInstanceOf[AnyRef])) return true
      if (!other.isInstanceOf[SessionHandle]) return false
      if (!super.equals(other)) return false
      val o = other.asInstanceOf[SessionHandle]
      if (handleId != o.handleId) return false
      true
    }

    override def hashCode(): Int = {
      var result = super.hashCode()
      result = 31 * result + handleId.hashCode()
      result
    }
  }

  @toString(includeFieldNames = true)
  class OperationHandle(
    private val opType: OperationType,
    private val hasResultSet: Boolean = false,
    override val handleId: HandleIdentifier = new HandleIdentifier()
  ) extends Handle(handleId) {

    def this(bOperationHandle: BOperationHandle) = {
      this(
        OperationType.getOperationType(bOperationHandle.getOperationType),
        bOperationHandle.getHasResultSet,
        new HandleIdentifier(bOperationHandle.getOperationId)
      )
    }

    def toBOperationHandle(): BOperationHandle =
      BOperationHandle
        .newBuilder()
        .setHasResultSet(hasResultSet)
        .setOperationId(handleId.toBHandleIdentifier())
        .setOperationType(BOperationType.forNumber(opType.id))
        .build()

    override def hashCode(): Int = {
      val prime = 31
      var result = super.hashCode()
      result = prime * result + opType.hashCode()
      result
    }

    override def equals(other: Any): Boolean = {
      if (this.eq(other.asInstanceOf[AnyRef])) {
        return true
      }
      if (!super.equals(other)) {
        return false
      }
      if (!other.isInstanceOf[OperationHandle]) {
        return false
      }
      val o = other.asInstanceOf[OperationHandle]
      opType == o.opType
    }
  }

}
