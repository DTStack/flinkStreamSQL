/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.dirty;

import com.dtstack.flink.sql.config.DirtyConfig;
import com.dtstack.flink.sql.exception.ParseOrWriteRecordException;
import com.dtstack.flink.sql.util.DateUtil;
import com.dtstack.flink.sql.util.FileSystemUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import java.io.IOException;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The class handles dirty data management
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DirtyDataManager {

    private static final EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags = EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH);

    private static final String FIELD_DELIMITER = "\u0001";
    private static final String LINE_DELIMITER = "\n";

    private static final ObjectMapper objMapper = new ObjectMapper();
    private Optional<FSDataOutputStream> fsOutputStream;
    private Map<String, Object> hadoopConfig;
    private String defaultSavePath = "/dirtydata";

    public DirtyDataManager(DirtyConfig dirtyConfig) {
        String path = dirtyConfig.getPath();
        if (!StringUtils.isEmpty(path)) {
            this.defaultSavePath = path;
        }

        Map<String, Object> hadoopConfig = dirtyConfig.getHadoopConfig();
        if (!MapUtils.isEmpty(hadoopConfig)) {
            this.hadoopConfig = hadoopConfig;
        }
    }

    public Optional<FSDataOutputStream> createFsOutputStream(String prefix, String jobId) {
        if (null == hadoopConfig) {
            return Optional.empty();
        }

        try {
            String location = defaultSavePath + "/" + jobId + "/" + prefix + "_" + UUID.randomUUID() + ".txt";
            FileSystem fs = FileSystemUtil.getFileSystem(hadoopConfig, null, jobId, "dirty");
            Path dataSavePath = new Path(location);
            fsOutputStream = Optional.of(fs.create(dataSavePath, true));
            return fsOutputStream;
        } catch (Exception e) {
            throw new RuntimeException("Open dirty manager error", e);
        }
    }


    public void writeData(String content, ParseOrWriteRecordException ex) {
        try {
            if (fsOutputStream.isPresent()) {
                String line = StringUtils.join(new String[]{content, objMapper.writeValueAsString(ex.toString()), DateUtil.timestampToString(new Date())}, FIELD_DELIMITER);
                fsOutputStream.get().writeChars(line);
                fsOutputStream.get().writeChars(LINE_DELIMITER);
                DFSOutputStream dfsOutputStream = (DFSOutputStream) fsOutputStream.get().getWrappedStream();
                dfsOutputStream.hsync(syncFlags);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (fsOutputStream.isPresent()) {
            try {
                fsOutputStream.get().flush();
                fsOutputStream.get().close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
