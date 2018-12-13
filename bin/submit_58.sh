#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
export JAVA_HOME=/opt/soft/jdk/jdk1.8.0_66
export SQL_HOME="$(cd "`dirname "$0"`"/..; pwd)"

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  JAVA_RUN="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    JAVA_RUN="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

JAR_DIR=$SQL_HOME/lib/*
CLASS_NAME=com.dtstack.flink.sql.launcher.LauncherMain
PARAM=$@
echo ${PARAM}
PARAM=${PARAM//time_characteristic/time.characteristic}
PARAM=${PARAM//sql_checkpoint_interval/sql.checkpoint.interval}
PARAM=${PARAM//sql_checkpoint_mode/sql.checkpoint.mode}
PARAM=${PARAM//sql_checkpoint_cleanup_mode/sql.checkpoint.cleanup.mode}
PARAM=${PARAM//parallelism/sql.env.parallelism}
echo ${PARAM} 
#export HADOOP_USER_NAME
if [ "$1" == "-hadoop_user_name" ];then
   echo "export HADOOP_USER_NAME="$2
   export HADOOP_USER_NAME=$2
fi
echo "sql submit ..."
$JAVA_RUN -cp $JAR_DIR $CLASS_NAME ${PARAM} & 
