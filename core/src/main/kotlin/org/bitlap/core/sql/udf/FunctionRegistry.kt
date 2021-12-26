package org.bitlap.core.sql.udf

import org.apache.calcite.schema.impl.AggregateFunctionImpl
import org.apache.calcite.schema.impl.ScalarFunctionImpl
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.InferTypes
import org.apache.calcite.sql.type.OperandTypes
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.util.Optionality
import org.bitlap.common.utils.PreConditions
import java.util.concurrent.ConcurrentHashMap

/**
 * manage functions.
 */
object FunctionRegistry {

    private val functions: ConcurrentHashMap<String, SqlFunction> = ConcurrentHashMap()

    init {
        register(
            UdfHello(),
            UdfIf(),
        )
        register(
            UdafBMSum(),
            UdafBMCountDistinct(),
        )
    }

    /**
     * register user defined aggregate functions
     */
    fun register(vararg func: UDAF<*, *, *>): FunctionRegistry {
        func.forEach { register(it) }
        return this
    }

    fun register(func: UDAF<*, *, *>): FunctionRegistry {
        val name = PreConditions.checkNotBlank(func.name).trim()
        if (functions.containsKey(name)) {
            throw IllegalArgumentException("$name function already exists.")
        }
        functions[name] = SqlUserDefinedAggFunction(
            SqlIdentifier(func.name, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            func.resultType,
            InferTypes.FIRST_KNOWN,
            OperandTypes.operandMetadata(
                func.inputTypes.map { it.family },
                { t -> func.inputTypes.map { t.createSqlType(it) } },
                { i -> "$i" },
                { true }
            ),
            AggregateFunctionImpl.create(func::class.java),
            false, false, Optionality.FORBIDDEN
        )
        return this
    }

    /**
     * register user defined functions
     */
    fun register(vararg func: UDF): FunctionRegistry {
        func.forEach { register(it) }
        return this
    }

    fun register(func: UDF): FunctionRegistry {
        val name = PreConditions.checkNotBlank(func.name)
        if (functions.containsKey(name)) {
            throw IllegalArgumentException("$name function already exists.")
        }
        functions[name] = SqlUserDefinedFunction(
            SqlIdentifier(func.name, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            func.resultType,
            InferTypes.FIRST_KNOWN,
            OperandTypes.operandMetadata(
                func.inputTypes.map { it.family },
                { t -> func.inputTypes.map { t.createSqlType(it) } },
                { i -> "$i" },
                { true }
            ),
            ScalarFunctionImpl.create(func::class.java, "eval")
        )
        return this
    }

    fun sqlFunctions() = this.functions.values
    fun getFunction(name: String) = this.functions[name]
    fun contains(name: String) = this.functions.containsKey(name)
}
