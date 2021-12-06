package org.bitlap.core.sql.rule

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataType
import org.bitlap.core.sql.rel.BitlapTableScan

/**
 * convert row any type to actually type from child node
 */
class BitlapRowTypeConverter : AbsRelRule(BitlapTableScan::class.java, "BitlapRowTypeConverter") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        rel as BitlapTableScan

        // convert parent node
        val parent = rel.parent
        if (parent == null || !hasAnyType(parent.rowType)) {
            return rel
        }
        // current node cannot contain any type
        if (hasAnyType(rel.rowType)) {
            return rel
        }

        // TODO
        return rel
    }

    private fun hasAnyType(rowType: RelDataType): Boolean {
        return rowType.fieldList.any { it.type.sqlTypeName.name.uppercase() == "ANY" }
    }
}
