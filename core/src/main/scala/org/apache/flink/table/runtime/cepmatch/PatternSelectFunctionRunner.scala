package org.apache.flink.table.runtime.cepmatch

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

/**
  * PatternSelectFunctionRunner with [[Row]] input and [[CRow]] output.
  */
class PatternSelectFunctionRunner(
                                   name: String,
                                   code: String)
  extends PatternSelectFunction[Row, CRow]
    with Compiler[PatternSelectFunction[Row, Row]] {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private var outCRow: CRow = _

  private var function: PatternSelectFunction[Row, Row] = _

  def init(): Unit = {
    LOG.debug(s"Compiling PatternSelectFunction: $name \n\n Code:\n$code")
    val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
    LOG.debug("Instantiating PatternSelectFunction.")
    function = clazz.newInstance()
  }

  override def select(pattern: util.Map[String, util.List[Row]]): CRow = {
    if (outCRow == null) {
      outCRow = new CRow(null, true)
    }

    if (function == null) {
      init()
    }

    outCRow.row = function.select(pattern)
    outCRow
  }
}
