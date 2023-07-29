/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test.sql

import io.kotest.matchers.shouldBe
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/14
 */
class CMDTest : BaseLocalFsTest(), SqlChecker {

    init {
        "test cmd sql" {
            sql("run example 'xxx'") shouldBe listOf(listOf("hello 'xxx'"))
        }
    }
}
