/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.hint.RelHint
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

import java.util.List as JList

/**
 * Bitlap table scan with push down filters
 */
class BitlapTableFilterScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: JList[RelHint],
    table: RelOptTable,
    val timeFilter: PruneTimeFilter,
    val pruneFilter: PrunePushedFilter,
    val isAlwaysFalse: Boolean,
    _parent: RelNode = null
) extends BitlapTableScan(cluster, traitSet, hints, table, _parent) {

    override def explainTerms(pw: RelWriter): RelWriter = {
        return super.explainTerms(pw)
            .item("timeFilter", timeFilter)
            .item("pruneFilter", pruneFilter)
    }

    override def withHints(hintList: JList[RelHint]): RelNode = {
        return BitlapTableFilterScan(getCluster, getTraitSet, hintList, getTable, timeFilter, pruneFilter, isAlwaysFalse, parent)
    }

    override def withTable(table: RelOptTable): BitlapTableFilterScan = {
        return BitlapTableFilterScan(getCluster, getTraitSet, getHints, table, timeFilter, pruneFilter, isAlwaysFalse, parent)
    }

    override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
        return this
    }
}
