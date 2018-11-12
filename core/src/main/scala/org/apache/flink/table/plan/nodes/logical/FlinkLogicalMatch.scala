package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Match
import org.apache.calcite.rel.logical.LogicalMatch
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.plan.nodes.FlinkConventions


class FlinkLogicalMatch(
                         cluster: RelOptCluster,
                         traitSet: RelTraitSet,
                         input: RelNode,
                         rowType: RelDataType,
                         pattern: RexNode,
                         strictStart: Boolean,
                         strictEnd: Boolean,
                         patternDefinitions: util.Map[String, RexNode],
                         measures: util.Map[String, RexNode],
                         after: RexNode,
                         subsets: util.Map[String, _ <: util.SortedSet[String]],
                         allRows: Boolean,
                         partitionKeys: util.List[RexNode],
                         orderKeys: RelCollation,
                         interval: RexNode)
  extends Match(
    cluster,
    traitSet,
    input,
    rowType,
    pattern,
    strictStart,
    strictEnd,
    patternDefinitions,
    measures,
    after,
    subsets,
    allRows,
    partitionKeys,
    orderKeys,
    interval)
    with FlinkLogicalRel {

  override def copy(
                     input: RelNode,
                     rowType: RelDataType,
                     pattern: RexNode,
                     strictStart: Boolean,
                     strictEnd: Boolean,
                     patternDefinitions: util.Map[String, RexNode],
                     measures: util.Map[String, RexNode],
                     after: RexNode,
                     subsets: util.Map[String, _ <: util.SortedSet[String]],
                     allRows: Boolean,
                     partitionKeys: util.List[RexNode],
                     orderKeys: RelCollation,
                     interval: RexNode): Match = {
    new FlinkLogicalMatch(
      cluster,
      traitSet,
      input,
      rowType,
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      measures,
      after,
      subsets,
      allRows,
      partitionKeys,
      orderKeys,
      interval)
  }
}

private class FlinkLogicalMatchConverter
  extends ConverterRule(
    classOf[LogicalMatch],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalMatchConverter") {

  override def convert(rel: RelNode): RelNode = {
    val logicalMatch = rel.asInstanceOf[LogicalMatch]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(logicalMatch.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalMatch(
      rel.getCluster,
      traitSet,
      newInput,
      logicalMatch.getRowType,
      logicalMatch.getPattern,
      logicalMatch.isStrictStart,
      logicalMatch.isStrictEnd,
      logicalMatch.getPatternDefinitions,
      logicalMatch.getMeasures,
      logicalMatch.getAfter,
      logicalMatch.getSubsets,
      logicalMatch.isAllRows,
      logicalMatch.getPartitionKeys,
      logicalMatch.getOrderKeys,
      logicalMatch.getInterval)
  }
}

object FlinkLogicalMatch {
  val CONVERTER: ConverterRule = new FlinkLogicalMatchConverter()
}
