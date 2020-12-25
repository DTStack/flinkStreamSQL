/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.sink.aws;

/**
 * @author tiezhu
 * date 2020/12/1
 * company dtstack
 */
public class AwsConstantKey {

    public static final String PARALLELISM_KEY = "parallelism";

    public static final String ACCESS_KEY = "accessKey";
    public static final String SECRET_KEY = "secretKey";
    public static final String BUCKET_KEY = "bucket";
    public static final String HOST_NAME = "hostname";
    public static final String STORAGE_TYPE = "storageType";
    public static final String BUCKET_ACL = "bucketAcl";
    public static final String OBJECT_NAME = "objectName";

    public static final String PUBLIC_READ_WRITE = "public-read-write";
    public static final String PRIVATE = "private";
    public static final String PUBLIC_READ = "public-read";
    public static final String AUTHENTICATED_READ = "authenticated-read";
    public static final String LOG_DELIVERY_READ = "log-delivery-write";
    public static final String BUCKET_OWNER_READER = "bucket-owner-read";
    public static final String BUCKET_OWNER_FULL_CONTROL = "bucket-owner-full-control";

    public static final String STANDARD = "standard";
    public static final String STANDARD_IA = "standard_ia";
    public static final String REDUCED_REDUNDANCY = "reduced_redundancy";
    public static final String GLACIER = "glacier";

}
