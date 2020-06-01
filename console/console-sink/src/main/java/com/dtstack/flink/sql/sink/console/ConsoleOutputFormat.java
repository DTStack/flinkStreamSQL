/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.sink.console;

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.console.table.TablePrintUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reason:
 * Date: 2018/12/19
 *
 * @author xuqianjin
 */
public class ConsoleOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleOutputFormat.class);

    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        initMetric();
    }

    @Override
    public void writeRecord(Tuple2 tuple2) throws IOException {
        LOG.info("received oriainal data:{}" + tuple2);
        Tuple2<Boolean, Row> tupleTrans = tuple2;
        Boolean retract = tupleTrans.getField(0);
        if (!retract) {
            return;
        }

        Row record = tupleTrans.getField(1);
        if (record.getArity() != fieldNames.length) {
            return;
        }

        List<String[]> data = new ArrayList<>();
        data.add(fieldNames);
        String[] recordStr = new String[record.getArity()];
        for (int i=0; i < record.getArity(); i++) {
            recordStr[i] = (String.valueOf(record.getField(i)));
        }
        data.add(recordStr);
        TablePrintUtil.build(data).print();

        outRecords.inc();
    }

    @Override
    public void close() throws IOException {

    }

    private ConsoleOutputFormat() {
    }

    public static ConsoleOutputFormatBuilder buildOutputFormat() {
        return new ConsoleOutputFormatBuilder();
    }

    public static class ConsoleOutputFormatBuilder {

        private final ConsoleOutputFormat format;

        protected ConsoleOutputFormatBuilder() {
            this.format = new ConsoleOutputFormat();
        }

        public ConsoleOutputFormatBuilder setFieldNames(String[] fieldNames) {
            format.fieldNames = fieldNames;
            return this;
        }

        public ConsoleOutputFormatBuilder setFieldTypes(TypeInformation<?>[] fieldTypes) {
            format.fieldTypes = fieldTypes;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured RetractConsoleCOutputFormat
         */
        public ConsoleOutputFormat finish() {
            return format;
        }
    }
}
