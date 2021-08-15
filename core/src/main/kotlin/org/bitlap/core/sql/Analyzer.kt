package org.bitlap.core.sql

import arrow.core.None
import arrow.core.Option
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelRoot
import org.bitlap.core.sql.analyzer.RuleChecker
import org.bitlap.core.sql.analyzer.RuleResolveWithMetric

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/12
 */
class Analyzer {

    private val rules = listOf(
        RuleChecker(),
        RuleResolveWithMetric(),
    )

    fun analyze(root: RelRoot): RelNode {
        AnalyzerContext.reset()
        try {
            val plan = root.rel
            return this.rules.fold(plan) { p, r ->
                r.execute(p)
            }
        } finally {
            AnalyzerContext.reset()
        }
    }
}

data class AnalyzerContext(val root: Option<RelRoot> = None) {

    companion object {
        private val context = object : ThreadLocal<AnalyzerContext>() {
            override fun initialValue(): AnalyzerContext = AnalyzerContext()
        }

        fun get() = context.get()
        fun reset() = context.remove()
    }
}
