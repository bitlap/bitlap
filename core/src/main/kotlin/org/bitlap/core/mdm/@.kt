package org.bitlap.core.mdm

import org.bitlap.common.BitlapIterator

/**
 * fetch function
 */
fun fetch(ctx: FetchContext.() -> Unit): BitlapIterator<Array<*>> {
    val context = FetchContext().apply(ctx)
    val plan = context.findBestPlan()
    return plan.execute(context)
}
