package org.bitlap.core.sql.rule

import cn.hutool.core.util.ReflectUtil
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rel.logical.LogicalTableScan
import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rel.BitlapProject
import org.bitlap.core.sql.rel.BitlapSort
import org.bitlap.core.sql.rel.BitlapTableScan

/**
 * Convert calcite rel node to bitlap rel node.
 */
class BitlapRelConverter : AbsRelRule(RelNode::class.java, "BitlapRelConverter") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        val root = call.planner.root.clean()
        // only convert root rel node
        if (rel != root) {
            return rel
        }
        return this.convert00(rel, call)
    }

    private fun convert00(rel: RelNode, call: RelOptRuleCall): RelNode {
        return when (rel) {
            // has been converted
            is BitlapNode -> {
                rel
            }
            is HepRelVertex -> {
                // next parent should be HepRelVertex's parent and next level should be current level, because it's a wrapper
                rel.also {
                    ReflectUtil.invoke<Void>(it, "replaceRel", this.convert00(it.currentRel, call))
                }
            }
            is LogicalSort -> {
                this.convert00(rel.input, call).injectParent {
                    BitlapSort(rel.cluster, rel.traitSet, it, rel.collation, rel.offset, rel.fetch)
                }
            }
            is LogicalAggregate -> {
                this.convert00(rel.input, call).injectParent {
                    BitlapAggregate(
                        rel.cluster, rel.traitSet, rel.hints, it,
                        rel.groupSet, rel.groupSets, rel.aggCallList
                    )
                }
            }
            is LogicalProject -> {
                this.convert00(rel.input, call).injectParent {
                    BitlapProject(rel.cluster, rel.traitSet, rel.hints, it, rel.projects, rel.rowType)
                }
            }
            is LogicalFilter -> {
                this.convert00(rel.input, call).injectParent {
                    BitlapFilter(rel.cluster, rel.traitSet, it, rel.condition, rel.variablesSet)
                }
            }
            is LogicalTableScan -> {
                BitlapTableScan(rel.cluster, rel.traitSet, rel.hints, rel.table)
            }
            else -> {
                when (rel) {
                    is SingleRel -> {
                        this.convert00(rel.input, call).injectParent {
                            rel.replaceInput(0, it)
                            rel
                        }
                    }
                    else -> {
                        throw IllegalArgumentException("Invalid converted rel node: ${rel.digest}")
                    }
                }
            }
        }
    }
}
