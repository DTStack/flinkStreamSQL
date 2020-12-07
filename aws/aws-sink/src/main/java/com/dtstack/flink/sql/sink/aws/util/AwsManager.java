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

package com.dtstack.flink.sql.sink.aws.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.dtstack.flink.sql.sink.aws.AwsConstantKey;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author tiezhu
 * date 2020/12/1
 * company dtstack
 * <p>
 * 一些针对s3的操作api
 */
public class AwsManager {
    private static final Logger LOG = LoggerFactory.getLogger(AwsManager.class);

    /**
     * 创建s3 client
     *
     * @param accessKey accessKey
     * @param secretKey secretKey
     * @param hostname  hostname
     * @return 客户端
     */
    public static AmazonS3Client initClient(String accessKey,
                                            String secretKey,
                                            String hostname) {
        ClientConfiguration configuration = new ClientConfiguration();
        // TODO 这个参数要对外开发出去
        configuration.setSignerOverride("S3SignerType");
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3Client client = new AmazonS3Client(credentials, configuration);
        client.setEndpoint(hostname);
        return client;
    }

    /**
     * 创建bucket
     *
     * @param bucketName   bucketName，不能为空
     * @param client       客户端，必须可用
     * @param readWrite    读写权限，默认私有
     * @param storageClass 存储类型，有STANDARD 【标准存储】 和 STANDARD_IA 【低频存储】 默认标准存储
     */
    public static void createBucket(String bucketName,
                                    AmazonS3Client client,
                                    String readWrite,
                                    String storageClass) {
        if (StringUtils.isBlank(readWrite)) {
            client.createBucket(bucketName);
            return;
        }

        CreateBucketRequest request = new CreateBucketRequest(bucketName);

        // TODO 完善多个类型
        if (readWrite.equalsIgnoreCase(AwsConstantKey.PUBLIC_READ_WRITE)) {
            request.setCannedAcl(CannedAccessControlList.PublicReadWrite);
        }

        // TODO 完善多个存储类型
        if (storageClass.equalsIgnoreCase(AwsConstantKey.STANDARD)) {
            request.setStorageClass(StorageClass.Standard);
        }
    }

    /**
     * 给bucket设置访问权限CannedAccess
     *
     * @param client     客户端
     * @param bucketName bucketName
     * @param aclName    权限名，权限名分为CannedAccessControlList 与AccessControlList 两种格式。
     */
    public static void setBucketCannedAccessControl(AmazonS3Client client,
                                                    String bucketName,
                                                    String aclName) {
        client.setBucketAcl(bucketName, CannedAccessControlList.valueOf(aclName));
    }

    /**
     * 给bucket 设置访问权限AccessControl
     *
     * @param client     客户端
     * @param bucketName bucketName
     * @param aclName    权限名
     */
    public static void setBucketAccessControl(AmazonS3Client client,
                                              String bucketName,
                                              String aclName) {
        AccessControlList list = new AccessControlList();
        // set owner
        Owner owner = new Owner("owner-id", "owner-displayName");
        list.setOwner(owner);

        // set owner id
        CanonicalGrantee grantee = new CanonicalGrantee("owner-id");

        // set owner display name
        grantee.setDisplayName("user-displayName");
        // set access of acl
        //TODO 完善Permission选择
        list.grantPermission(grantee, Permission.parsePermission(aclName));
        SetBucketAclRequest request = new SetBucketAclRequest(bucketName, list);
        client.setBucketAcl(request);
    }

    /**
     * 获取当前 Object 字节位数，如果 Object 不存在，那么返回 0
     *
     * @param bucket     bucket
     * @param objectName object name
     * @param client     client
     * @return object bytes 位数
     */
    public static long getObjectPosition(String bucket, String objectName, AmazonS3Client client) {
        if (!client.doesObjectExist(bucket, objectName)) {
            return 0;
        }

        S3Object object = client.getObject(bucket, objectName);
        ObjectMetadata objectMetadata = object.getObjectMetadata();
        return objectMetadata.getContentLength();
    }
}
