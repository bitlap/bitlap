/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.schema.impl.AggregateFunctionImpl
import org.apache.calcite.schema.impl.ScalarFunctionImpl
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.InferTypes
import org.apache.calcite.sql.type.OperandTypes
import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.util.Optionality
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.sql.udf.aggr.BMCountAggr
import org.bitlap.core.sql.udf.aggr.BMCountDistinctAggr
import org.bitlap.core.sql.udf.aggr.BMSumAggr
import org.bitlap.core.sql.udf.date.DateFormat
import org.bitlap.core.sql.udf.expr.Hello
import org.bitlap.core.sql.udf.expr.If
import java.util.concurrent.ConcurrentHashMap

/**
 * manage functions.
 */
object FunctionRegistry {

    private val typeFactory = JavaTypeFactoryImpl()
    private val functions: ConcurrentHashMap<String, SqlFunction> = ConcurrentHashMap()

    init {
        // UDF
        register(Hello())
        register(If())
        register(DateFormat())

        // udaf
        register(BMSumAggr())
        register(BMCountAggr())
        register(BMCountDistinctAggr())
    }

    /**
     * register user defined functions and aggregate functions
     */
    @JvmStatic
    fun register(vararg func: UDF): FunctionRegistry {
        func.forEach { register(it.name, it) }
        return this
    }

    @JvmStatic
    fun register(name: String, func: UDF): FunctionRegistry {
        val cleanName = PreConditions.checkNotBlank(name).trim()
        if (functions.containsKey(cleanName)) {
            throw IllegalArgumentException("$cleanName function already exists.")
        }
        when (func) {
            is UDAF<*, *, *> ->
                functions[cleanName] = SqlUserDefinedAggFunction(
                    SqlIdentifier(cleanName, SqlParserPos.ZERO),
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

            else ->
                functions[cleanName] = SqlUserDefinedFunction(
                    SqlIdentifier(cleanName, SqlParserPos.ZERO),
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
        }
        return this
    }

    /**
     * register function from lambda
     */
    fun <R> register(name: String, func: Function0<R>) = this.register0(name, func)
    fun <P1, R> register(name: String, func: Function1<P1, R>) = this.register0(name, func)
    fun <P1, P2, R> register(name: String, func: Function2<P1, P2, R>) = this.register0(name, func)
    fun <P1, P2, P3, R> register(name: String, func: Function3<P1, P2, P3, R>) = this.register0(name, func)
    fun <P1, P2, P3, P4, R> register(name: String, func: Function4<P1, P2, P3, P4, R>) = this.register0(name, func)
    fun <P1, P2, P3, P4, P5, R> register(name: String, func: Function5<P1, P2, P3, P4, P5, R>) = this.register0(name, func)

    fun <R> register0(name: String, func: Function<R>): FunctionRegistry {
        val cleanName = PreConditions.checkNotBlank(name).trim()
        if (functions.containsKey(cleanName)) {
            throw IllegalArgumentException("$cleanName function already exists.")
        }
        val methods = func::class.java.methods.filter { it.name == "invoke" }
        val method = if (methods.size == 1) {
            methods.first()
        } else {
            methods.firstOrNull { it.returnType != Object::class.java } ?: methods.first()
        }
        val inputTypes = method.parameterTypes.map { typeFactory.createJavaType(it)!! }
        functions[cleanName] = SqlUserDefinedFunction(
            SqlIdentifier(cleanName, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(typeFactory.createJavaType(method.returnType)),
            InferTypes.FIRST_KNOWN,
            OperandTypes.operandMetadata(
                inputTypes.map { it.sqlTypeName.family },
                { inputTypes },
                { i -> "$i" },
                { true }
            ),
            ScalarFunctionImpl.createUnsafe(method)
        )

        return this
    }

    fun sqlFunctions() = this.functions.values
    fun getFunction(name: String) = this.functions[name]
    fun contains(name: String) = this.functions.containsKey(name)
}
