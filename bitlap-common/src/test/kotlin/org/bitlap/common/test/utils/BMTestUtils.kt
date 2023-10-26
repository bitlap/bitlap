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
package org.bitlap.common.test.utils

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.bitmap.RBM
import kotlin.math.max
import kotlin.random.Random

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/4
 */
object BMTestUtils {

    /**
     * Create a [RBM] with [num] ids,
     * [max] should be grater than 1000
     */
    fun randomRBM(num: Int, max: Int = Short.MAX_VALUE.toInt(), duplicate: Boolean = false): RBM {
        val rbm = RBM()
        val total = max(num, 0)
        val maxId = max(max, 1000)
        var current = 0
        while (current < total) {
            val id = Random.nextInt(maxId)
            if (duplicate) {
                rbm.add(id)
                current ++
            } else {
                if (!rbm.contains(id)) {
                    rbm.add(id)
                    current ++
                }
            }
        }
        return rbm
    }

    /**
     * Create a [BBM] with [num] ids and random([maxBucket]) bucket
     * [max] should be grater than 1000
     */
    fun randomBBM(maxBucket: Int, num: Int, max: Int = Short.MAX_VALUE.toInt(), duplicate: Boolean = false): BBM {
        val bbm = BBM()
        val total = max(num, 0)
        val maxId = max(max, 1000)
        var current = 0
        while (current < total) {
            val bucket = Random.nextInt(maxBucket)
            val id = Random.nextInt(maxId)
            if (duplicate) {
                bbm.add(bucket, id)
                current ++
            } else {
                if (!bbm.contains(id)) {
                    bbm.add(bucket, id)
                    current ++
                }
            }
        }
        return bbm
    }

    /**
     * Create a [CBM] with [num] ids and random([maxBucket]) bucket, random([maxCount]) count
     * [max] should be grater than 1000
     */
    fun randomCBM(maxCount: Long, maxBucket: Int, num: Int, max: Int = Short.MAX_VALUE.toInt()): CBM {
        val cbm = CBM()
        val total = max(num, 0)
        val maxId = max(max, 1000)
        var current = 0
        while (current < total) {
            val bucket = Random.nextInt(maxBucket)
            val id = Random.nextInt(maxId)
            val cnt = Random.nextLong(maxCount) + 1
            cbm.add(bucket, id, cnt)
            current ++
        }
        return cbm
    }
}
