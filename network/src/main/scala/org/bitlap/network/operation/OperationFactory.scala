/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.operation

import org.bitlap.network.operation.operations.Operation
import org.bitlap.network.session.Session

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
trait OperationFactory {

  def create(
    parentSession: Session,
    opType: OperationType.OperationType,
    hasResultSet: Boolean = false
  ): Operation

}
