/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.common.conf

import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * bitlap conf keys
 */
object BitlapConfKeys {

    /**
     * Project name, default is bitlap
     */
    @JvmField
    val PROJECT_NAME = BitlapConfKey("bitlap.project.name", "bitlap")
        .desc("Bitlap project name.")
        .sys("bitlap.project.name")
        .env("BITLAP_PROJECT_NAME")

    /**
     * Root dir
     */
    @JvmField
    val ROOT_DIR = BitlapConfKey<String>("bitlap.root.dir")
        .sys("bitlap.root.dir")
        .env("BITLAP_ROOT_DIR")
        .validator(Validators.NOT_BLANK)

    /**
     * Local dir
     */
    @JvmField
    val LOCAL_DIR = BitlapConfKey<String>("bitlap.local.dir")
        .sys("bitlap.local.dir")
        .env("BITLAP_LOCAL_DIR")
        .validator(Validators.NOT_BLANK)

    /**
     * Node address, rpc/raft/http configuration, etc.
     */
    @JvmField
    val NODE_HOST = BitlapConfKey<String>("bitlap.node.host")
        .sys("bitlap.node.host")
        .env("BITLAP_NODE_HOST")
        .validator(Validators.NOT_BLANK)

    @JvmField
    val NODE_RAFT_DIR = BitlapConfKey<String>("bitlap.node.raft.dir")
        .sys("bitlap.node.raft.dir")
        .env("BITLAP_NODE_RAFT_DIR")
        .validator(Validators.NOT_BLANK)

    @JvmField
    val NODE_RAFT_GROUP_ID = BitlapConfKey("bitlap.node.raft.group.id", "bitlap")
        .sys("bitlap.node.raft.group.id")
        .env("BITLAP_NODE_RAFT_GROUP_ID")

    @JvmField
    val NODE_RAFT_HOST = BitlapConfKey<String>("bitlap.node.raft.host")
        .sys("bitlap.node.raft.host")
        .env("BITLAP_NODE_RAFT_HOST")
        .validator(Validators.NOT_BLANK)

    @JvmField
    val NODE_RAFT_PEERS = BitlapConfKey<String>("bitlap.node.raft.peers")
        .sys("bitlap.node.raft.peers")
        .env("BITLAP_NODE_RAFT_PEERS")
        .validator(Validators.NOT_BLANK)

    @JvmField
    val NODE_RAFT_TIMEOUT = BitlapConfKey<Duration>("bitlap.node.raft.timeout", 5.seconds /* 5s */)
        .sys("bitlap.node.raft.timeout")
        .env("BITLAP_NODE_RAFT_TIMEOUT")
        .validator(Validators.gt(0.seconds))

    @JvmField
    val NODE_HTTP_HOST = BitlapConfKey<String>("bitlap.node.http.host")
        .sys("bitlap.node.http.host")
        .env("BITLAP_NODE_HTTP_HOST")
        .validator(Validators.NOT_BLANK)

    @JvmField
    val NODE_HTTP_THREADS = BitlapConfKey("bitlap.node.http.threads", 16)
        .sys("bitlap.node.http.threads")
        .env("BITLAP_NODE_HTTP_THREADS")
        .validator(Validators.gt(0))

    @JvmField
    val NODE_SESSION_EXPIRY_PERIOD = BitlapConfKey<Duration>("bitlap.node.session.expiry.period", 30.minutes /* 30m */)
        .sys("bitlap.node.session.expiry.period")
        .env("BITLAP_NODE_SESSION_EXPIRY_PERIOD")
        .validator(Validators.gt(0.seconds))

    @JvmField
    val NODE_SESSION_EXPIRY_INTERVAL = BitlapConfKey<Duration>("bitlap.node.session.expiry.interval", 5.seconds /* 5s */)
        .sys("bitlap.node.session.expiry.interval")
        .env("BITLAP_NODE_SESSION_EXPIRY_INTERVAL")
        .validator(Validators.gt(0.seconds))
}
