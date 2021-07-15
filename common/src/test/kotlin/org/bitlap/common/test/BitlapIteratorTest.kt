package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.BitlapBatchIterator

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/16
 */
class BitlapIteratorTest : StringSpec({

    "Test simple BitlapBatchIterator" {
        val batchIterator = object : BitlapBatchIterator<Int>() {
            private val pars = (1..100).chunked(10).iterator()
            override fun hasNext(): Boolean = pars.hasNext() || super.hasNext()
            override fun nextBatch(): List<Int> = pars.next()
            override fun next(): Int = super.next() * 2
        }
        batchIterator.asSequence().sum() shouldBe 5050 * 2
    }
})
