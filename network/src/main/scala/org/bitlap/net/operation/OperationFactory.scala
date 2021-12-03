package org.bitlap.net.operation

import org.bitlap.net.operation.operations.Operation
import org.bitlap.net.session.Session

/**
 *
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
trait OperationFactory {

  def create(parentSession: Session, opType: OperationType.OperationType, hasResultSet: Boolean = false): Operation

}
