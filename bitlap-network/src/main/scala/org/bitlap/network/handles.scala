/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network

import java.nio.ByteBuffer
import java.util.UUID

import org.bitlap.network.driver_proto.*
import org.bitlap.network.enumeration.OperationType

import com.google.protobuf.ByteString

/** 客户端操作的媒介（≈句柄）
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
object handles:

  /** 抽象处理器
   *
   *  @param handleId
   *    唯一ID
   */
  sealed abstract class Handle(val handleId: HandleIdentifier = new HandleIdentifier()):

    // super不能直接引用handleId属性
    def getHandleId(): HandleIdentifier = handleId

    def this(bHandleIdentifier: BHandleIdentifier) =
      this(new HandleIdentifier(bHandleIdentifier))

    override def hashCode(): Int =
      val prime  = 31
      var result = 1
      result = prime * result + handleId.hashCode()
      result

    override def equals(other: Any): Boolean =
      if this.eq(other.asInstanceOf[AnyRef]) then return true
      if other == null then return false
      if !other.isInstanceOf[Handle] then return false
      val otherHandle = other.asInstanceOf[Handle]
      if handleId != otherHandle.handleId then return false
      true

  /** 统一标识符定义
   *
   *  @param publicId
   *  @param secretId
   */
  final class HandleIdentifier(var publicId: UUID = UUID.randomUUID(), var secretId: UUID = UUID.randomUUID()):

    def this(bHandleId: BHandleIdentifier) =
      this()
      var bb = ByteBuffer.wrap(bHandleId.guid.toByteArray)
      this.publicId = new UUID(bb.getLong, bb.getLong)
      bb = ByteBuffer.wrap(bHandleId.secret.toByteArray)
      this.secretId = new UUID(bb.getLong, bb.getLong)

    def toBHandleIdentifier(): BHandleIdentifier =
      val guid     = new Array[Byte](16)
      val secret   = new Array[Byte](16)
      val guidBB   = ByteBuffer.wrap(guid)
      val secretBB = ByteBuffer.wrap(secret)
      guidBB.putLong(this.publicId.getMostSignificantBits)
      guidBB.putLong(this.publicId.getLeastSignificantBits)
      secretBB.putLong(this.secretId.getMostSignificantBits)
      secretBB.putLong(this.secretId.getLeastSignificantBits)
      BHandleIdentifier(guid = ByteString.copyFrom(guid), secret = ByteString.copyFrom(secret))

    override def hashCode(): Int =
      val prime  = 31
      var result = 1
      result = prime * result + publicId.hashCode()
      result = prime * result + secretId.hashCode()
      result

    override def equals(other: Any): Boolean =
      if this.eq(other.asInstanceOf[AnyRef]) then return true
      if other == null then return false
      if !other.isInstanceOf[HandleIdentifier] then return false
      val o = other.asInstanceOf[HandleIdentifier]
      if publicId != o.publicId then return false
      if secretId != o.secretId then return false
      true

  /** 会话处理器句柄
   *
   *  @param handleId
   */
  final class SessionHandle(override val handleId: HandleIdentifier) extends Handle(handleId):

    def this(bSessionHandle: BSessionHandle) =
      this(new HandleIdentifier(bSessionHandle.getSessionId))

    def toBSessionHandle(): BSessionHandle =
      BSessionHandle(Some(super.getHandleId().toBHandleIdentifier()))

    override def equals(other: Any): Boolean =
      if this.eq(other.asInstanceOf[AnyRef]) then return true
      if !other.isInstanceOf[SessionHandle] then return false
      if !super.equals(other) then return false
      val o = other.asInstanceOf[SessionHandle]
      if handleId != o.handleId then return false
      true

    override def hashCode(): Int =
      var result = super.hashCode()
      result = 31 * result + handleId.hashCode()
      result

  final class OperationHandle(
    private val opType: OperationType,
    val hasResultSet: Boolean = false,
    override val handleId: HandleIdentifier = new HandleIdentifier())
      extends Handle(handleId):

    def this(bOperationHandle: BOperationHandle) =
      this(
        OperationType.toOperationType(bOperationHandle.operationType),
        bOperationHandle.hasResultSet,
        new HandleIdentifier(bOperationHandle.getOperationId)
      )

    def toBOperationHandle(): BOperationHandle =
      BOperationHandle(
        hasResultSet = hasResultSet,
        operationId = Some(handleId.toBHandleIdentifier()),
        operationType = BOperationType.fromValue(opType.value)
      )

    override def hashCode(): Int =
      val prime  = 31
      var result = super.hashCode()
      result = prime * result + opType.hashCode()
      result

    override def equals(other: Any): Boolean =
      if this.eq(other.asInstanceOf[AnyRef]) then return true
      if !super.equals(other) then return false
      if !other.isInstanceOf[OperationHandle] then return false
      val o = other.asInstanceOf[OperationHandle]
      opType == o.opType
