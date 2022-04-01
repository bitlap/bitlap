/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.core

import org.bitlap.network.operation.{ operations, OperationFactory }
import org.bitlap.network.operation.OperationType.OperationType
import org.bitlap.network.session.Session

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapOperationFactory extends OperationFactory {

  override def create(
    parentSession: Session,
    opType: OperationType,
    hasResultSet: Boolean
  ): operations.Operation =
    BitlapOperation(parentSession, opType, hasResultSet)
}
