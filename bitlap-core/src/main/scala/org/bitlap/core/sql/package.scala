/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import org.apache.calcite.sql.`type`.ReturnTypes
import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

extension (sqlTypeName: SqlTypeName) {

  /** infer sql type from `SqlTypeName`
   */
  def infer(): SqlReturnTypeInference = ReturnTypes.explicit(sqlTypeName)
}
