package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamMatch
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalMatch
import org.apache.flink.table.plan.schema.RowSchema

class DataStreamMatchRule
  extends ConverterRule(
    classOf[FlinkLogicalMatch],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamMatchRule") {

  override def convert(rel: RelNode): RelNode = {
    val logicalMatch: FlinkLogicalMatch = rel.asInstanceOf[FlinkLogicalMatch]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convertInput: RelNode =
      RelOptRule.convert(logicalMatch.getInput, FlinkConventions.DATASTREAM)

    new DataStreamMatch(
      rel.getCluster,
      traitSet,
      convertInput,
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
      logicalMatch.getInterval,
      new RowSchema(logicalMatch.getRowType),
      new RowSchema(logicalMatch.getInput.getRowType))
  }
}

object DataStreamMatchRule {
  val INSTANCE: RelOptRule = new DataStreamMatchRule
}
