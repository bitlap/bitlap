package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.BitlapProperties

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/22
 */
class BitlapPropertiesTest : StringSpec({

    "test simple bitlap properties" {
        BitlapProperties.getRootDir() shouldBe "/tmp/data/bitlap"
    }
})
