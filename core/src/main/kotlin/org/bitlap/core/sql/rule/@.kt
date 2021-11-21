package org.bitlap.core.sql.rule

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.bitlap.core.sql.rel.BitlapNode

/**
 * Entrance functions for sql rules.
 */
val RULES = listOf(
    BitlapRelConverter(),
    BitlapFilterTableScanRule(),
    ValidRule(),
    BitlapTableConverter(),
)

/**
 * clean HepRelVertex wrapper
 */
fun RelNode?.clean(): RelNode? {
    return when (this) {
        is HepRelVertex -> this.currentRel
        else -> this
    }
}

/**
 * inject parent node
 */
fun RelNode.injectParent(parent: (RelNode) -> RelNode): RelNode {
    val p = parent(this)
    when {
        this is BitlapNode ->
            this.parent = p
        this is HepRelVertex && this.currentRel is BitlapNode ->
            (this.currentRel as BitlapNode).parent = p
    }
    return p
}
