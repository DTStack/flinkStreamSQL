package org.apache.flink.table.runtime.cepmatch

import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

/**
  * IterativeConditionRunner with [[Row]] value.
  */
class IterativeConditionRunner(
                                name: String,
                                code: String)
  extends IterativeCondition[Row]
    with Compiler[IterativeCondition[Row]]{

  val LOG = LoggerFactory.getLogger(this.getClass)

  // IterativeCondition will be serialized as part of state,
  // so make function as transient to avoid ClassNotFoundException when restore state,
  // see FLINK-6939 for details
  @transient private var function: IterativeCondition[Row] = _

  def init(): Unit = {
    LOG.debug(s"Compiling IterativeCondition: $name \n\n Code:\n$code")
    // We cannot get user's classloader currently, see FLINK-6938 for details
    val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
    LOG.debug("Instantiating IterativeCondition.")
    function = clazz.newInstance()
  }

  override def filter(value: Row, ctx: IterativeCondition.Context[Row]): Boolean = {

    if (function == null) {
      init()
    }

    function.filter(value, ctx)
  }
}