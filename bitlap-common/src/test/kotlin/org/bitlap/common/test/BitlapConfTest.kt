/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.extensions.system.withEnvironment
import io.kotest.matchers.shouldBe
import org.bitlap.common.BitlapConf
import org.bitlap.common.conf.BitlapConfKeys
import kotlin.time.Duration.Companion.seconds

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/22
 */
class BitlapConfTest : StringSpec({

    "test simple bitlap configuration" {
        val conf = BitlapConf()
        conf.get(BitlapConfKeys.PROJECT_NAME) shouldBe "bitlap"
        conf.get(BitlapConfKeys.ROOT_DIR) shouldBe "/usr/local/var/bitlap"
        conf.get(BitlapConfKeys.LOCAL_DIR) shouldBe "/usr/local/var/bitlap"
        conf.get(BitlapConfKeys.NODE_HOST) shouldBe "127.0.0.1:23333"
        conf.get(BitlapConfKeys.NODE_RAFT_HOST) shouldBe "127.0.0.1:24333"
        conf.get(BitlapConfKeys.NODE_RAFT_PEERS) shouldBe "127.0.0.1:24333"
        conf.get(BitlapConfKeys.NODE_RAFT_TIMEOUT) shouldBe 5.seconds
    }

    "test bitlap configuration with system properties and OS environment variables" {
        System.setProperty(BitlapConfKeys.NODE_HOST.getSysKey(), "127.0.0.1:13333")
        withEnvironment(BitlapConfKeys.NODE_RAFT_HOST.getEnvKey(), "127.0.0.1:14333") {
            val conf = BitlapConf()
            conf.get(BitlapConfKeys.NODE_HOST) shouldBe "127.0.0.1:13333"
            conf.get(BitlapConfKeys.NODE_RAFT_HOST) shouldBe "127.0.0.1:14333"
        }
    }

    "test bitlap configuration with parameters" {
        val conf = BitlapConf(mapOf(BitlapConfKeys.NODE_RAFT_TIMEOUT.key to "10s"))
        conf.get(BitlapConfKeys.NODE_RAFT_TIMEOUT) shouldBe 10.seconds
    }
})
