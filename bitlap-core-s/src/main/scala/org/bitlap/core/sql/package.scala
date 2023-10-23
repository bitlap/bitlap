package org.bitlap.core

import org.apache.calcite.sql.`type`.ReturnTypes
import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

package object sql {
  
  extension (sqlTypeName: SqlTypeName) {

    /**
     * infer sql type from `SqlTypeName`
     */
    def infer(): SqlReturnTypeInference = ReturnTypes.explicit(sqlTypeName)
  }
}

