package org.bitlap.common.model

import org.bitlap.common.bitmap.BBM

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/10
 */
data class Metric(val key: String, val measure: Double) {
    private var hasAgg: Boolean = false
    private lateinit var bbm: BBM

    constructor(key: String, bbm: BBM): this(key, 0.0) {
        hasAgg = true
        this.bbm = bbm
    }
}
