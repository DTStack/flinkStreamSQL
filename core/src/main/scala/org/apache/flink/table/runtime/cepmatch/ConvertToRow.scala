package org.apache.flink.table.runtime.cepmatch

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

/**
  * MapFunction convert CRow to Row.
  */
class ConvertToRow extends MapFunction[CRow, Row] {
  override def map(value: CRow): Row = {
    value.row
  }
}