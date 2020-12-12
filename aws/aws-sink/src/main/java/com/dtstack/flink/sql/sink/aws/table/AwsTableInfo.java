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

package com.dtstack.flink.sql.sink.aws.table;

import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;

/**
 * @author tiezhu
 * date 2020/12/1
 * company dtstack
 */
public class AwsTableInfo extends AbstractTargetTableInfo {

    private static final String CURRENT_TYPE = "aws";

    private String accessKey;

    private String secretKey;

    private String hostname;

    private String bucketName;

    /**
     * 写入s3的objectName
     */
    private String objectName;

    /**
     * 文件存储的类型，分为Standard【标准存储】和 Standard-ia【低频存储】
     */
    private String storageType;

    /**
     * 设置bucket的访问权限，有基本的两种，CannedAccessControlList和AccessControlList
     */
    private String bucketAcl;

    private String userId;

    private String userDisplayName;

    private String ownerId;

    private String ownerDisplayName;

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public String getBucketAcl() {
        return bucketAcl;
    }

    public void setBucketAcl(String bucketAcl) {
        this.bucketAcl = bucketAcl;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getOwnerDisplayName() {
        return ownerDisplayName;
    }

    public void setOwnerDisplayName(String ownerDisplayName) {
        this.ownerDisplayName = ownerDisplayName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(accessKey, "S3 field of AccessKey is required!");
        Preconditions.checkNotNull(secretKey, "S3 field of SecretKey is required!");
        Preconditions.checkNotNull(bucketName, "S3 field of BucketName is required!");
        Preconditions.checkNotNull(objectName, "S3 field of ObjectName is required!");

        return true;
    }

    @Override
    public String getType() {
        return CURRENT_TYPE;
    }
}
