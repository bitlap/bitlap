package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.BitlapConf

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/22
 */
class BitlapConfTest : StringSpec({

    "test simple bitlap configuration" {
        val conf = BitlapConf()
        conf.get(BitlapConf.DEFAULT_ROOT_DIR_DATA) shouldBe "/tmp/data/bitlap_data"
        conf.get(BitlapConf.DEFAULT_ROOT_DIR_LOCAL) shouldBe "/tmp/data/bitlap"
        conf.get(BitlapConf.DEFAULT_ROOT_DIR_LOCAL_META) shouldBe "/tmp/data/bitlap/meta"
    }
})
