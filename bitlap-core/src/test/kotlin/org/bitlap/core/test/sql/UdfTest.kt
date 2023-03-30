/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.test.sql

import org.bitlap.core.sql.udf.FunctionRegistry
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class UdfTest : BaseLocalFsTest(), SqlChecker {

    class TestHello : Function1<String, String> {
        override fun invoke(p1: String): String {
            return "hello $p1"
        }
    }

    init {

        "test lambda udf registry" {
            // TODO: not support kotlin lambda and inline class, should clean it like apache spark
            // for example, kotlin compiler generate class is: org.bitlap.core.test.sql.UdfTest$1$1
            FunctionRegistry.register("test_hello") { s: String ->
                "hello $s"
            }

            // support common class and inner class
            FunctionRegistry.register("test_hello2", TestHello())
            checkRows(
                "select test_hello2('a')",
                listOf(listOf("hello a"))
            )
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

        "test date_format udf" {
            checkRows(
                """
                    select 
                      date_format(1672502400000, 'yyyyMMdd') d1
                     ,date_format(1672502400000, 'yyyy-MM-dd HH:mm:ss') d2
                     ,date_format('2023-01-01', 'yyyyMMdd') d3
                     ,date_format('2023-01-01T00:00:00.000+08:00', 'yyyy-MM-dd HH:mm:ss') d3
                """.trimIndent(),
                listOf(listOf("20230101", "2023-01-01 00:00:00", "20230101", "2023-01-01 00:00:00"))
            )
        }
    }
}
