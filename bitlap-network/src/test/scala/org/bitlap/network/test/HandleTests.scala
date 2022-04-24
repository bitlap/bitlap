/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.test

import org.bitlap.network.handles.{ HandleIdentifier, SessionHandle }
import org.bitlap.network.{ handles, OperationType }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class HandleTests extends AnyFlatSpec with Matchers {

  "test operationHandle" should "ok" in {
    val handleId = new handles.HandleIdentifier()
    val op = new handles.OperationHandle(
      OperationType.EXECUTE_STATEMENT,
      hasResultSet = true,
      handleId = handleId
    )

    (op.toBOperationHandle().getOperationId == handleId
      .toBHandleIdentifier()) shouldBe true

    (op.toBOperationHandle().getOperationId.guid == handleId
      .toBHandleIdentifier()
      .guid) shouldBe true

    (op.toBOperationHandle().getOperationId.secret == handleId
      .toBHandleIdentifier()
      .secret) shouldBe true

    OperationType.getOperationType(
      op.toBOperationHandle().operationType
    ) == OperationType.EXECUTE_STATEMENT shouldBe true

    val h1 = new handles.OperationHandle(op.toBOperationHandle())
    h1 == op shouldBe true
  }

  "test sessionHandle" should "ok" in {
    val handleId = new HandleIdentifier()
    val op = new SessionHandle(handleId)

    (op.toBSessionHandle().getSessionId == handleId
      .toBHandleIdentifier()) shouldBe true

    (op.toBSessionHandle().getSessionId.guid == handleId
      .toBHandleIdentifier()
      .guid) shouldBe true

    (op.toBSessionHandle().getSessionId.secret == handleId
      .toBHandleIdentifier()
      .secret) shouldBe true

    new SessionHandle(op.toBSessionHandle()) == op shouldBe true
  }
}
