package org.bitlap.network.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.network.core.HandleIdentifier
import org.bitlap.network.core.OperationType
import org.bitlap.network.core.operation.OperationHandle

/**
 *
 * @author 梦境迷离
 * @since 2021/9/6
 * @version 1.0
 */
class OperationTests : StringSpec({

    "test operationHandle" {
        val handleId = HandleIdentifier()
        val op = OperationHandle(OperationType.EXECUTE_STATEMENT, hasResultSet = true, handleId = handleId)

        (op.toBOperationHandle().operationId == op.handleId.toBHandleIdentifier()) shouldBe true

        (op.toBOperationHandle().operationId.guid == op.handleId.toBHandleIdentifier().guid) shouldBe true

        (op.toBOperationHandle().operationId.secret == op.handleId.toBHandleIdentifier().secret) shouldBe true

        (op.toBOperationHandle().operationTypeValue == OperationType.EXECUTE_STATEMENT.toBOperationType().number) shouldBe true

        (OperationHandle(op.toBOperationHandle()) == op) shouldBe true
    }
})
