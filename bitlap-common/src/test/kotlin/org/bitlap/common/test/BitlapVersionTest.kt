/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.BitlapVersionInfo

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/22
 */
class BitlapVersionTest : StringSpec({

    "test bitlap version generate" {
        val version = BitlapVersionInfo.getVersion()
        version.substring(0, 1) shouldBe "0"
    }
})
