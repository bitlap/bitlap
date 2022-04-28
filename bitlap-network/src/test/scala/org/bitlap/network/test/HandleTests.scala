/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.test

import junit.framework.TestCase
import org.bitlap.network.handles.{ HandleIdentifier, SessionHandle }
import org.bitlap.network.{ handles, OperationType }
import org.junit.Test
import junit.framework.TestCase.assertTrue

/**
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class HandleTests extends TestCase("HandleTests") {

  @Test
  def testOperationHandle = {
    val handleId = new handles.HandleIdentifier()
    val op = new handles.OperationHandle(
      OperationType.EXECUTE_STATEMENT,
      hasResultSet = true,
      handleId = handleId
    )

    assertTrue(
      op.toBOperationHandle().getOperationId == handleId
        .toBHandleIdentifier()
    )

    assertTrue(
      op.toBOperationHandle().getOperationId.guid == handleId
        .toBHandleIdentifier()
        .guid
    )

    assertTrue(
      op.toBOperationHandle().getOperationId.secret == handleId
        .toBHandleIdentifier()
        .secret
    )

    assertTrue(
      OperationType.getOperationType(
        op.toBOperationHandle().operationType
      ) == OperationType.EXECUTE_STATEMENT
    )

    val h1 = new handles.OperationHandle(op.toBOperationHandle())
    assertTrue(h1 == op)
  }

  @Test
  def testSessionHandle = {
    val handleId = new HandleIdentifier()
    val op = new SessionHandle(handleId)

    assertTrue(
      op.toBSessionHandle().getSessionId == handleId
        .toBHandleIdentifier()
    )

    assertTrue(
      op.toBSessionHandle().getSessionId.guid == handleId
        .toBHandleIdentifier()
        .guid
    )

    assertTrue(
      op.toBSessionHandle().getSessionId.secret == handleId
        .toBHandleIdentifier()
        .secret
    )

    assertTrue(new SessionHandle(op.toBSessionHandle()) == op)
  }
}
