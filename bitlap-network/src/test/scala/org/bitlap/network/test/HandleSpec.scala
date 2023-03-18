/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network.test

import junit.framework.TestCase
import junit.framework.TestCase.assertTrue
import org.bitlap.network.enumeration.OperationType
import org.bitlap.network.handles.{ HandleIdentifier, SessionHandle }
import org.bitlap.network.handles
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertFalse

/** @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class HandleSpec extends TestCase("HandleTests") {

  @Test
  def testOperationHandle = {
    val handleId = new handles.HandleIdentifier()
    val op = new handles.OperationHandle(
      OperationType.ExecuteStatement,
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
      OperationType.toOperationType(
        op.toBOperationHandle().operationType
      ) == OperationType.ExecuteStatement
    )

    val h1 = new handles.OperationHandle(op.toBOperationHandle())
    assertTrue(h1 == op)
  }

  @Test
  def testSessionHandle = {
    val handleId = new HandleIdentifier()
    val op       = new SessionHandle(handleId)

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

  @Test
  def testInnerMethod(): Unit = {
    val handleId = new HandleIdentifier()
    val op1      = new SessionHandle(handleId)
    val op2      = new SessionHandle(handleId)
    val op3      = new SessionHandle(new HandleIdentifier())
    assertTrue(op1.equals(op2))
    assertTrue(op1.hashCode() == op2.hashCode())
    assertFalse(op1.hashCode() == op3.hashCode())
    assertTrue(op3.toString.contains("handleId"))
  }
}
