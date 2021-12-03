package org.bitlap.core.sql.rel

import org.apache.calcite.rel.RelNode

/**
 * bitlap rel node
 */
interface BitlapNode : RelNode {

    /**
     * parent rel node
     */
    var parent: RelNode?
}
