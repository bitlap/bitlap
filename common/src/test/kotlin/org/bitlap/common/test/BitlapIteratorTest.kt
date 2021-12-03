package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.BitlapIterator

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/16
 */
class BitlapIteratorTest : StringSpec({

    "Test simple BitlapIterator" {
        val iterator = BitlapIterator.of((1..100))
        iterator.asSequence().sum() shouldBe 5050
        val batchIterator = BitlapIterator.batch((1..100), 10)
        batchIterator.asSequence()
            .map { it * 2 }
            .sum() shouldBe 5050 * 2
    }
})
