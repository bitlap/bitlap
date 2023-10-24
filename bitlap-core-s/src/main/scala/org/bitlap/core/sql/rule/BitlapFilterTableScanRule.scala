/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import org.bitlap.core.extension._
import org.bitlap.core.sql.FilterOp
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rel.BitlapTableFilterScan
import org.bitlap.core.sql.rel.BitlapTableScan
import org.bitlap.core.sql.rule.shuttle.RexInputRefShuttle

import org.apache.calcite.DataContexts
import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexExecutorImpl
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.RangeSets
import org.apache.calcite.util.Sarg
import org.apache.calcite.util.mapping.Mappings

/** see [org.apache.calcite.rel.rules.FilterTableScanRule]
 */
class BitlapFilterTableScanRule extends AbsRelRule(classOf[BitlapFilter], "BitlapFilterTableScanRule") {

  override def convert0(_rel: RelNode, call: RelOptRuleCall): RelNode = {
    val rel = _rel.asInstanceOf[BitlapFilter]
    rel.getInput.clean() match {
      case _: BitlapTableFilterScan => rel // has been converted
      case scan: BitlapTableScan =>
        val relBuilder = call.builder()
        val rexBuilder = RexBuilder(relBuilder.getTypeFactory)
        val projects   = scan.identity()
        val mapping    = Mappings.target(projects, scan.getTable.getRowType.getFieldCount)
        val filter     = RexUtil.apply(mapping.inverse(), rel.getCondition)

        // simplify filter, try to predict is the where expression is always false
        val oFilter = RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
          .simplifyUnknownAsFalse(filter)

        // if condition is always false, just push to [BitlapTableConverter] and return
        if (oFilter.isAlwaysFalse) {
          BitlapTableFilterScan(
            scan.getCluster,
            scan.getTraitSet,
            scan.getHints,
            scan.getTable,
            PruneTimeFilter(),
            PrunePushedFilter(),
            true,
            rel.parent
          )
        }
        // push down filter
        else {
          val dnfFilter = RexUtil.toDnf(rexBuilder, oFilter)
          // prune pure filter to push down, and retain the rest
          // current not support OR expression
          val (timeFilter, pruneFilter, unPruneFilter) = this.pruneFilter(dnfFilter, scan.getRowType, rexBuilder)
          val inputScan = BitlapTableFilterScan(
            scan.getCluster,
            scan.getTraitSet,
            scan.getHints,
            scan.getTable,
            timeFilter,
            pruneFilter,
            false,
            rel.parent
          )
          if (unPruneFilter == null) {
            inputScan
          } else {
            rel.copy(rel.getTraitSet, inputScan, unPruneFilter)
          }
        }
      case _ => rel
    }
  }

  private def pruneFilter(
    filter: RexNode,
    rowType: RelDataType,
    rexBuilder: RexBuilder
  ): (PruneTimeFilter, PrunePushedFilter, RexNode) = {
    val timeFilter = new PruneTimeFilter()
    val timeField  = rowType.getFieldList.asScala.find(_.getName == Keyword.TIME)

    val pruneFilter   = new PrunePushedFilter()
    val fieldIndexMap = rowType.getFieldList.asScala.groupBy(_.getIndex).map(kv => kv._1 -> kv._2.head)

    val unPruneFilter = filter.accept(new RexShuttle() {
      override def visitCall(call: RexCall): RexNode = {
        call.getKind match {
          case k if k == SqlKind.OR => throw new NotImplementedError("OR expression is not supported now.") // TODO
          case k if k == SqlKind.AND =>
            val operands = ListBuffer[RexNode]().also { it => visitList(call.operands, it.toList.asJava) }
              .filter(_ != null)
            if (operands.size == 2) {
              rexBuilder.makeCall(call.op, operands.asJava)
            } else {
              operands.headOption.orNull
            }
          // x (=, <>, >, >=, <, <=) xxx
          // x in (xx, xxx, ...)
          // x between xx and xxx
          case k if k == SqlKind.COMPARISON || k == SqlKind.SEARCH =>
            val left  = call.operands.get(0)
            val right = call.operands.get(1)
            (left, right) match {
              case _: (RexInputRef, RexLiteral) | _: (RexLiteral, RexInputRef) =>
                val (_ref, _refValue) = if (left.isInstanceOf[RexInputRef]) left -> right else right -> left
                val ref               = _ref.asInstanceOf[RexInputRef]
                val refValue          = _refValue.asInstanceOf[RexLiteral]

                val inputField = fieldIndexMap(ref.getIndex)
                if (timeField == inputField) {
                  timeFilter.add(
                    inputField.getName,
                    resolveFilter(call, inputField, rowType, rexBuilder),
                    call.toString.replace("$" + inputField.getIndex, inputField.getName)
                  )
                } else {
                  val (values, op) = getLiteralValue(refValue, call.getKind.sql.toLowerCase())
                  pruneFilter.add(
                    inputField.getName,
                    op,
                    values,
                    resolveFilter(call, inputField, rowType, rexBuilder),
                    call.toString.replace("$" + inputField.getIndex, inputField.getName)
                  )
                }
                null
              case _ =>
                val refs = RexInputRefShuttle.of(call).getInputRefs
                if (refs.size == 1) {
                  val inputField = fieldIndexMap(refs.head.getIndex)
                  if (timeField == inputField) {
                    timeFilter.add(
                      inputField.getName,
                      resolveFilter(call, inputField, rowType, rexBuilder),
                      call.toString.replace("$" + inputField.getIndex, inputField.getName)
                    )
                    null
                  } else {
                    call
                  }
                } else {
                  call
                }
            }
          // x is [not] null
          case k if k == SqlKind.IS_NULL || k == SqlKind.IS_NOT_NULL => throw NotImplementedError("TODO")
          // x like xxx
          case k if k == SqlKind.LIKE => throw NotImplementedError("TODO")
          case _                      => call
        }
      }
    })
    (timeFilter, pruneFilter, unPruneFilter)
  }

  /** resolve time filter to normal function
   */
  def resolveFilter[T](
    inputFilter: RexNode,
    inputField: RelDataTypeField,
    rowType: RelDataType,
    builder: RexBuilder
  ): T => Boolean = {
    // reset field index to 0
    val filter = RexUtil.apply(
      Mappings.target(
        Map(Int.box(inputField.getIndex) -> Int.box(0)).asJava,
        Int.box(inputField.getIndex + 1),
        Int.box(1)
      ),
      inputFilter
    )
    // convert to normal function
    val executor = RexExecutorImpl.getExecutable(builder, List(filter).asJava, rowType).getFunction

    (it: T) => {
      // why inputRecord? see DataContextInputGetter
      val input = DataContexts.of(Map("inputRecord" -> Array(it.asInstanceOf[Any])).asJava)
      executor.apply(input).asInstanceOf[Array[_]].head.asInstanceOf[Boolean]
    }
  }

  private def getLiteralValue(literal: RexLiteral, op: String): (List[String], FilterOp) = {
    literal.getValue match {
      case _: Sarg[_] =>
        val values   = ListBuffer[NlsString]()
        var filterOp = FilterOp.EQUALS
        RangeSets.forEach(
          literal.getValue.asInstanceOf[Sarg[NlsString]].rangeSet,
          new RangeSets.Consumer[NlsString] {
            override def all(): Unit = throw IllegalStateException("Illegal compare all.")
            override def atLeast(lower: NlsString): Unit = {
              values += lower
              filterOp = FilterOp.GREATER_EQUALS_THAN
            }

            override def atMost(upper: NlsString): Unit = {
              values += upper
              filterOp = FilterOp.LESS_EQUALS_THAN
            }

            override def greaterThan(lower: NlsString): Unit = {
              values += lower
              filterOp = FilterOp.GREATER_THAN
            }

            override def lessThan(upper: NlsString): Unit = {
              values += upper
              filterOp = FilterOp.LESS_THAN
            }

            override def singleton(value: NlsString): Unit = {
              values += value
              filterOp = FilterOp.EQUALS
            }

            override def closed(lower: NlsString, upper: NlsString): Unit = {
              values += lower
              values += upper
              filterOp = FilterOp.CLOSED
            }

            override def closedOpen(lower: NlsString, upper: NlsString): Unit = {
              values += lower
              values += upper
              filterOp = FilterOp.CLOSED_OPEN
            }

            override def openClosed(lower: NlsString, upper: NlsString): Unit = {
              values += lower
              values += upper
              filterOp = FilterOp.OPEN_CLOSED
            }

            override def open(lower: NlsString, upper: NlsString): Unit = {
              values += lower
              values += upper
              filterOp = FilterOp.OPEN
            }
          }
        )
        values.map(_.getValue).toList -> filterOp
      case _ =>
        List(literal.getValueAs(classOf[String])) -> FilterOp.from(op)
    }
  }
}
