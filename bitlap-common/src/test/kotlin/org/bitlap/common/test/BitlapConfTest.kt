/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.test

import io.kotest.assertions.throwables.shouldThrow
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
        conf.get(BitlapConf.PROJECT_NAME) shouldBe "bitlap"
        conf.get(BitlapConf.ROOT_DIR_DATA) shouldBe "/tmp/data/bitlap_data"
        conf.get(BitlapConf.ROOT_DIR_LOCAL) shouldBe "/tmp/data/bitlap"
        shouldThrow<Exception> {
            conf.get(BitlapConf.NODE_BIND_HOST) shouldBe "127.0.0.1:23333"
        }
        System.setProperty(BitlapConf.NODE_BIND_HOST.getSysKey(), "127.0.0.1:23333")
        conf.get(BitlapConf.NODE_BIND_HOST) shouldBe "127.0.0.1:23333"
        conf.get(BitlapConf.NODE_BIND_PEERS) shouldBe "127.0.0.1:23333"
        conf.get(BitlapConf.NODE_RPC_TIMEOUT) shouldBe 3000L
        conf.get(BitlapConf.NODE_READ_TIMEOUT) shouldBe 10000L
    }

    "test bitlap configuration with parameters" {
        val conf = BitlapConf(mapOf(BitlapConf.NODE_RPC_TIMEOUT.key to "1000L"))
        conf.get(BitlapConf.NODE_RPC_TIMEOUT) shouldBe 1000L
    }
})
