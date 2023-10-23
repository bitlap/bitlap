/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rel

import org.apache.calcite.rel.RelNode

/**
 * bitlap rel node
 */
trait BitlapNode extends RelNode {

    /**
     * parent rel node
     */
    var parent: RelNode
}
