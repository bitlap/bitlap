/* Copyright (c) 2022 bitlap.org */
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
import java.util.concurrent.ConcurrentHashMap

/**
 * manage functions.
 */
object FunctionRegistry {

    private val typeFactory = JavaTypeFactoryImpl()
    private val functions: ConcurrentHashMap<String, SqlFunction> = ConcurrentHashMap()

    init {
        register(
            UdfHello(),
            UdfIf(),
            UdfDateFormat()
        )
        register(
            UdafBMSum(),
            UdafBMCount(),
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

    /**
     * register function from lambda
     */
    fun <R> register(_name: String, func: Function0<R>) = this.register0(_name, func)
    fun <P1, R> register(_name: String, func: Function1<P1, R>) = this.register0(_name, func)
    fun <P1, P2, R> register(_name: String, func: Function2<P1, P2, R>) = this.register0(_name, func)
    fun <P1, P2, P3, R> register(_name: String, func: Function3<P1, P2, P3, R>) = this.register0(_name, func)
    fun <P1, P2, P3, P4, R> register(_name: String, func: Function4<P1, P2, P3, P4, R>) = this.register0(_name, func)
    fun <P1, P2, P3, P4, P5, R> register(_name: String, func: Function5<P1, P2, P3, P4, P5, R>) = this.register0(_name, func)
    fun <R> register0(_name: String, func: Function<R>): FunctionRegistry {
        val name = PreConditions.checkNotBlank(_name).trim()
        if (functions.containsKey(name)) {
            throw IllegalArgumentException("$name function already exists.")
        }
        val methods = func::class.java.methods.filter { it.name == "invoke" }
        val method = if (methods.size == 1) {
            methods.first()
        } else {
            methods.firstOrNull { it.returnType != Object::class.java } ?: methods.first()
        }
        val inputTypes = method.parameterTypes.map { typeFactory.createJavaType(it)!! }
        functions[name] = SqlUserDefinedFunction(
            SqlIdentifier(name, SqlParserPos.ZERO),
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

    fun sqlValidatorFunctions(): () -> List<UdfValidator> = {
        this.functions.values.filter {
            when (it) {
                is UdfValidator -> true
                else -> false
            }
        }.map { it as UdfValidator }
    }
    fun getFunction(name: String) = this.functions[name]
    fun contains(name: String) = this.functions.containsKey(name)
}
