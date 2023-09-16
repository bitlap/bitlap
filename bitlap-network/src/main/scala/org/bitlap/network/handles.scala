/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.network

import java.nio.ByteBuffer
import java.util.UUID

import org.bitlap.common.utils.StringEx
import org.bitlap.network.Driver.*
import org.bitlap.network.enumeration.OperationType

import com.google.protobuf.ByteString

/** @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
object handles:

  sealed abstract class Handle(val handleId: HandleIdentifier = HandleIdentifier()):

    def getHandleId(): HandleIdentifier = handleId

    def this(bHandleIdentifier: BHandleIdentifier) =
      this(HandleIdentifier(bHandleIdentifier.value))

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

  final case class HandleIdentifier(value: String = StringEx.uuid(true))

  final case class SessionHandle(override val handleId: HandleIdentifier) extends Handle(handleId):

    def this(bSessionHandle: BSessionHandle) =
      this(HandleIdentifier(bSessionHandle.sessionId.map(_.value).orNull))

    def toBSessionHandle(): BSessionHandle =
      BSessionHandle(Some(BHandleIdentifier.of(handleId.value)))
  end SessionHandle

  final case class OperationHandle(
    private val opType: OperationType,
    hasResultSet: Boolean = false,
    override val handleId: HandleIdentifier = HandleIdentifier())
      extends Handle(handleId):

    def this(bOperationHandle: BOperationHandle) =
      this(
        OperationType.toOperationType(bOperationHandle.operationType),
        bOperationHandle.hasResultSet,
        HandleIdentifier(bOperationHandle.operationId.map(_.value).orNull)
      )

    def toBOperationHandle(): BOperationHandle =
      BOperationHandle(
        hasResultSet = hasResultSet,
        operationId = Some(BHandleIdentifier.of(handleId.value)),
        operationType = BOperationType.fromValue(opType.value)
      )
  end OperationHandle
