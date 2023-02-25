/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm

import org.bitlap.core.mdm.model.RowIterator

/**
 * fetch function
 */
fun fetch(ctx: FetchContext.() -> Unit): RowIterator {
    val context = FetchContext().apply(ctx)
    val plan = context.findBestPlan()
    return plan.execute(context)
}
