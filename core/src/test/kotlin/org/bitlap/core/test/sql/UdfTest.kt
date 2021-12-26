package org.bitlap.core.test.sql

import org.bitlap.core.sql.udf.FunctionRegistry
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class UdfTest : BaseLocalFsTest(), SqlChecker {

    init {

        "test lambda udf registry" {
            FunctionRegistry.register("test_hello") { s: String ->
                "hello $s"
            }
            // TODO: not support, shoud clean kotlin lambda like apache spark
            // for example, kotlin compiler generate class is: org.bitlap.core.test.sql.UdfTest$1$1
//            checkRows(
//                "select test_hello('a')",
//                listOf(listOf("hello a"))
//            )
        }

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
