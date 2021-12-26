package org.bitlap.core.test.sql

import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class UdfTest : BaseLocalFsTest(), SqlChecker {

    init {

        "test hello udf" {
            checkRows(
                "select hello(null), hello(1), hello('a')",
                listOf(listOf(null, "hello 1", "hello a"))
            )
        }

        "test if udf" {
            checkRows(
                """
                    select 
                      if(true, 'a', 'b'),
                      if(false, 'a', 'b'),
                      if(1=1, 'a', 'b'),
                      if(1=2, 'a', 'b')
                """.trimIndent(),
                listOf(listOf("a", "b", "a", "b"))
            )
        }
    }
}
