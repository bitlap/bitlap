package org.bitlap.core.test.sql

import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class UdfTest : BaseLocalFsTest(), SqlChecker {

    init {

        "test if udf" {
            sql("select if(1=1, 'a', 'b')").show()
        }
    }
}
