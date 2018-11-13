package org.apache.flink.table.runtime.cepmatch

import java.util

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.MatchCodeGenerator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

/**
  * An util class to generate match functions.
  * 1。IterativeCondition
  * 2。PatternSelectFunction
  * 3。PatternFlatSelectFunction
  */
object MatchUtil {

  private[flink] def generateIterativeCondition(
                                                 config: TableConfig,
                                                 inputType: RowSchema,
                                                 patternName: String,
                                                 patternNames: Seq[String],
                                                 patternDefinition: RexNode,
                                                 inputTypeInfo: TypeInformation[_]): IterativeCondition[Row] = {

    val generator = new MatchCodeGenerator(
      config, false, inputTypeInfo, patternNames, true, Some(patternName))
    val condition = generator.generateExpression(patternDefinition)
    val body =
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin

    val genCondition = generator.generateIterativeCondition("MatchRecognizeCondition", body)
    new IterativeConditionRunner(genCondition.name, genCondition.code)
  }

  private[flink] def generatePatternSelectFunction(
                                                    config: TableConfig,
                                                    returnType: RowSchema,
                                                    patternNames: Seq[String],
                                                    partitionKeys: util.List[RexNode],
                                                    measures: util.Map[String, RexNode],
                                                    inputTypeInfo: TypeInformation[_]): PatternSelectFunction[Row, CRow] = {

    val generator = new MatchCodeGenerator(config, false, inputTypeInfo, patternNames, false)

    val resultExpression = generator.generateSelectOutputExpression(
      partitionKeys,
      measures,
      returnType)
    val body =
      s"""
         |${resultExpression.code}
         |return ${resultExpression.resultTerm};
         |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternSelectFunction(
      "MatchRecognizePatternSelectFunction",
      body)
    new PatternSelectFunctionRunner(genFunction.name, genFunction.code)
  }

  private[flink] def generatePatternFlatSelectFunction(
                                                        config: TableConfig,
                                                        returnType: RowSchema,
                                                        patternNames: Seq[String],
                                                        partitionKeys: util.List[RexNode],
                                                        orderKeys: RelCollation,
                                                        measures: util.Map[String, RexNode],
                                                        inputTypeInfo: TypeInformation[_]): PatternFlatSelectFunction[Row, CRow] = {

    val generator = new MatchCodeGenerator(config, false, inputTypeInfo, patternNames, false)

    val resultExpression = generator.generateFlatSelectOutputExpression(
      partitionKeys,
      orderKeys,
      measures,
      returnType)
    val body =
      s"""
         |${resultExpression.code}
         |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternFlatSelectFunction(
      "MatchRecognizePatternFlatSelectFunction",
      body)
    new PatternFlatSelectFunctionRunner(genFunction.name, genFunction.code)
  }
}
