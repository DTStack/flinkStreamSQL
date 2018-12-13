package org.apache.flink.table.runtime.cepmatch

import java.util

import org.apache.flink.cep.PatternFlatSelectFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * PatternFlatSelectFunctionRunner with [[Row]] input and [[CRow]] output.
  */
class PatternFlatSelectFunctionRunner(
                                       name: String,
                                       code: String)
  extends PatternFlatSelectFunction[Row, CRow]
    with Compiler[PatternFlatSelectFunction[Row, Row]] {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private var cRowWrapper: CRowWrappingCollector = _

  private var function: PatternFlatSelectFunction[Row, Row] = _

  def init(): Unit = {
    LOG.debug(s"Compiling PatternFlatSelectFunction: $name \n\n Code:\n$code")
    val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
    LOG.debug("Instantiating PatternFlatSelectFunction.")
    function = clazz.newInstance()

    this.cRowWrapper = new CRowWrappingCollector()
  }

  override def flatSelect(
                           pattern: util.Map[String, util.List[Row]],
                           out: Collector[CRow]): Unit = {
    if (function == null) {
      init()
    }

    cRowWrapper.out = out
    function.flatSelect(pattern, cRowWrapper)
  }
}
