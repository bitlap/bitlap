/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.rule.shuttle

import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle

class RexInputRefShuttle private constructor(val call: RexNode) : RexShuttle() {

    private val refs = mutableSetOf<RexInputRef>()

    fun getInputRefs(): List<RexInputRef> {
        return refs.toList()
    }

    private fun init(): RexInputRefShuttle {
        return this.also { this.call.accept(this) }
    }

    override fun visitInputRef(inputRef: RexInputRef): RexNode {
        refs.add(inputRef)
        return super.visitInputRef(inputRef)
    }

    companion object {
        fun of(call: RexNode): RexInputRefShuttle {
            return RexInputRefShuttle(call).init()
        }
    }
}
