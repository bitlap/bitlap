package org.bitlap.core.sql

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
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
        return AnalyzerContext.use {
            it.root = Some(root)
            this.rules.fold(root.rel) { p, r ->
                r.execute(p)
            }
        }
    }
}

data class AnalyzerContext(var root: Option<RelRoot> = None) {

    companion object {
        private val context = object : ThreadLocal<AnalyzerContext>() {
            override fun initialValue(): AnalyzerContext = AnalyzerContext()
        }

        fun get(): AnalyzerContext = context.get()
        fun reset() = context.remove()

        fun <T> use(block: (AnalyzerContext) -> T): T {
            try {
                reset()
                return block(get())
            } finally {
                reset()
            }
        }
    }
}
