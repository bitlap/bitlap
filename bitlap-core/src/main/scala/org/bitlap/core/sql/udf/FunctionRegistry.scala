/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.sql.udf

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.sql.udf.aggr.BMCountAggr
import org.bitlap.core.sql.udf.aggr.BMCountDistinctAggr
import org.bitlap.core.sql.udf.aggr.BMSumAggr
import org.bitlap.core.sql.udf.date.DateFormat
import org.bitlap.core.sql.udf.expr.Hello
import org.bitlap.core.sql.udf.expr.If

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.schema.impl.AggregateFunctionImpl
import org.apache.calcite.schema.impl.ScalarFunctionImpl
import org.apache.calcite.sql.`type`.InferTypes
import org.apache.calcite.sql.`type`.OperandTypes
import org.apache.calcite.sql.`type`.ReturnTypes
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.util.Optionality

/** manage functions.
 */
object FunctionRegistry {

  private val typeFactory                                       = JavaTypeFactoryImpl()
  private val functions: ConcurrentHashMap[String, SqlFunction] = ConcurrentHashMap()

  {
    // UDF
    register(Hello(), If(), DateFormat())
    // udaf
    register(BMSumAggr(), BMCountAggr(), BMCountDistinctAggr())
  }

  /** register user defined functions and aggregate functions
   */
  def register(func: UDF*): this.type = {
    func.foreach(f => register(f.name, f))
    this
  }

  def register(name: String, func: UDF): this.type = {
    val cleanName = PreConditions.checkNotBlank(name).trim()
    if (functions.containsKey(cleanName)) {
      throw IllegalArgumentException(s"$cleanName function already exists.")
    }
    func match {
      case _: UDAF[_, _, _] =>
        functions.put(
          cleanName,
          SqlUserDefinedAggFunction(
            SqlIdentifier(cleanName, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            func.resultType,
            InferTypes.FIRST_KNOWN,
            OperandTypes.operandMetadata(
              func.inputTypes.map(_.getFamily).asJava,
              { t => func.inputTypes.map(t.createSqlType).asJava },
              { i => s"$i" },
              { _ => true }
            ),
            AggregateFunctionImpl.create(func.getClass),
            false,
            false,
            Optionality.FORBIDDEN
          )
        )

      case _ =>
        functions.put(
          cleanName,
          SqlUserDefinedFunction(
            SqlIdentifier(cleanName, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            func.resultType,
            InferTypes.FIRST_KNOWN,
            OperandTypes.operandMetadata(
              func.inputTypes.map(_.getFamily).asJava,
              { t => func.inputTypes.map(t.createSqlType).asJava },
              { i => s"$i" },
              { _ => true }
            ),
            ScalarFunctionImpl.create(func.getClass, "eval")
          )
        )
    }
    this
  }

  /** register function from lambda
   */
  def register[R](name: String, func: Function0[R])                                         = this.register0(name, func)
  def register[P1, R](name: String, func: Function1[P1, R])                                 = this.register0(name, func)
  def register[P1, P2, R](name: String, func: Function2[P1, P2, R])                         = this.register0(name, func)
  def register[P1, P2, P3, R](name: String, func: Function3[P1, P2, P3, R])                 = this.register0(name, func)
  def register[P1, P2, P3, P4, R](name: String, func: Function4[P1, P2, P3, P4, R])         = this.register0(name, func)
  def register[P1, P2, P3, P4, P5, R](name: String, func: Function5[P1, P2, P3, P4, P5, R]) = this.register0(name, func)

  def register0[R](name: String, func: AnyRef): this.type = {
    val cleanName = PreConditions.checkNotBlank(name).trim()
    if (functions.containsKey(cleanName)) {
      throw IllegalArgumentException(s"$cleanName function already exists.")
    }
    val methods = func.getClass.getMethods.filter(m => m.getName == "invoke" || m.getName == "apply")
    val method = if (methods.length == 1) {
      methods.head
    } else {
      methods.find(m => m.getReturnType != classOf[Object] || m.getReturnType != classOf[Any]).getOrElse(methods.head)
    }
    val inputTypes = method.getParameterTypes.map(typeFactory.createJavaType)
    functions.put(
      cleanName,
      SqlUserDefinedFunction(
        SqlIdentifier(cleanName, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(typeFactory.createJavaType(method.getReturnType)),
        InferTypes.FIRST_KNOWN,
        OperandTypes.operandMetadata(
          inputTypes.map(_.getSqlTypeName.getFamily).toList.asJava,
          { _ => inputTypes.toList.asJava },
          { i => s"$i" },
          { _ => true }
        ),
        ScalarFunctionImpl.createUnsafe(method)
      )
    )

    this
  }

  def sqlFunctions(): util.Collection[SqlFunction] = this.functions.values
  def getFunction(name: String): SqlFunction       = this.functions.get(name)
  def contains(name: String): Boolean              = this.functions.containsKey(name)
}
