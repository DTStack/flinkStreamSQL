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

package com.dtstack.flink.sql.side.elasticsearch6.util;

import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author yinxi
 * @date 2020/2/21 - 20:51
 */
public class Es6Util {

    private static final Logger LOG = LoggerFactory.getLogger(Es6Util.class);
    private static final String KEY_WORD_TYPE = ".keyword";
    private static final String APOSTROPHE = "'";

    // connect to the elasticsearch
    public static RestHighLevelClient getClient(String esAddress, Boolean isAuthMesh, String userName, String password) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] address = StringUtils.split(esAddress, ",");
        for (String addr : address) {
            String[] infoArray = StringUtils.split(addr, ":");
            int port = 9200;
            String host = infoArray[0].trim();
            if (infoArray.length > 1) {
                port = Integer.parseInt(infoArray[1].trim());
            }
            httpHostList.add(new HttpHost(host, port, "http"));
        }

        RestClientBuilder restClientBuilder = RestClient.builder(httpHostList.toArray(new HttpHost[0]));

        if (isAuthMesh) {
            // 进行用户和密码认证
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName.trim(), password.trim()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        RestHighLevelClient rhlClient = new RestHighLevelClient(restClientBuilder);

        if (LOG.isInfoEnabled()) {
            LOG.info("Pinging Elasticsearch cluster via hosts {} ...", httpHostList);
        }

        try {
            if (!rhlClient.ping(RequestOptions.DEFAULT)) {
                throw new RuntimeException("There are no reachable Elasticsearch nodes!");
            }
        } catch (IOException e) {
            LOG.warn("", e);
        }


        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", httpHostList.toString());
        }

        return rhlClient;
    }

    // add index and type to search request
    public static SearchRequest setSearchRequest(BaseSideInfo sideInfo) {
        SearchRequest searchRequest = new SearchRequest();
        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();
        // determine existence of index
        String index = tableInfo.getIndex();
        if (!StringUtils.isEmpty(index) || index.trim().length() > 0) {
            // strip leading and trailing spaces from a string
            String[] indexes = StringUtils.split(index, ",");
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = indexes[i].trim();
            }

            searchRequest.indices(indexes);
        }

        // determine existence of type
        String type = tableInfo.getEsType();
        if (!StringUtils.isEmpty(type) || type.trim().length() > 0) {
            // strip leading and trailing spaces from a string
            String[] types = StringUtils.split(type, ",");
            for (int i = 0; i < types.length; i++) {
                types[i] = types[i].trim();
            }

            searchRequest.types(types);
        }

        return searchRequest;
    }

    // build where cause
    public static BoolQueryBuilder setPredicateclause(BaseSideInfo sideInfo) {

        BoolQueryBuilder boolQueryBuilder = null;
        List<PredicateInfo> predicateInfoes = sideInfo.getSideTableInfo().getPredicateInfoes();
        if (predicateInfoes.size() > 0) {
            boolQueryBuilder = new BoolQueryBuilder();
            for (PredicateInfo info : predicateInfoes) {
                boolQueryBuilder = Es6Util.buildFilterCondition(boolQueryBuilder, info, sideInfo);
            }
        }

        return boolQueryBuilder;
    }

    // build filter condition
    public static BoolQueryBuilder buildFilterCondition(BoolQueryBuilder boolQueryBuilder, PredicateInfo info, BaseSideInfo sideInfo) {
        switch (info.getOperatorKind()) {
            case "IN":
                return boolQueryBuilder.must(QueryBuilders.termsQuery(textConvertToKeyword(info.getFieldName(), sideInfo), removeSpaceAndApostrophe(info.getCondition())));
            case "NOT_IN":
                return boolQueryBuilder.mustNot(QueryBuilders.termsQuery(textConvertToKeyword(info.getFieldName(), sideInfo), removeSpaceAndApostrophe(info.getCondition())));
            case ">=":
            case "GREATER_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gte(info.getCondition()));
            case ">":
            case "GREATER_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gt(info.getCondition()));
            case "<=":
            case "LESS_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lte(info.getCondition()));
            case "<":
            case "LESS_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lt(info.getCondition()));
            case "BETWEEN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gte(StringUtils.split(info.getCondition().toUpperCase(), "AND")[0].trim())
                        .lte(StringUtils.split(info.getCondition().toUpperCase(), "AND")[1].trim()));
            case "IS_NULL":
                return boolQueryBuilder.mustNot(QueryBuilders.existsQuery(info.getFieldName()));
            case "IS_NOT_NULL":
                return boolQueryBuilder.must(QueryBuilders.existsQuery(info.getFieldName()));
            case "=":
            case "EQUALS":
                if (StringUtils.isBlank(info.getCondition())) {
                    return boolQueryBuilder;
                }
                return boolQueryBuilder.must(QueryBuilders.termQuery(textConvertToKeyword(info.getFieldName(), sideInfo), removeSpaceAndApostrophe(info.getCondition())[0]));
            case "<>":
            case "NOT_EQUALS":
                return boolQueryBuilder.mustNot(QueryBuilders.termQuery(textConvertToKeyword(info.getFieldName(), sideInfo), removeSpaceAndApostrophe(info.getCondition())[0]));
            default:
                return boolQueryBuilder;
        }

    }

    // remove extra spaces and apostrophes
    public static String[] removeSpaceAndApostrophe(String str) {
        String[] split = StringUtils.split(str, ",");
        for (int i = 0; i < split.length; i++) {
            split[i] = StringUtils.trim(split[i]);
            if (StringUtils.startsWith(split[i], APOSTROPHE)) {
                split[i] = StringUtils.substring(split[i], 1, split[i].length() - 1);
            }
        }

        return split;
    }

    // prevent word segmentation
    public static String textConvertToKeyword(String fieldName, BaseSideInfo sideInfo) {
        String[] sideFieldTypes = sideInfo.getSideTableInfo().getFieldTypes();
        int fieldIndex = sideInfo.getSideTableInfo().getFieldList().indexOf(fieldName.trim());
        String fieldType = sideFieldTypes[fieldIndex];
        switch (fieldType.toLowerCase(Locale.ENGLISH)) {
            case "varchar":
            case "char":
            case "text":
                return fieldName + KEY_WORD_TYPE;
            default:
                return fieldName;
        }
    }

    public static Object getTarget(Object obj, String targetType) {
        switch (targetType.toLowerCase(Locale.ENGLISH)) {

            case "smallint":
            case "smallintunsigned":
            case "tinyint":
            case "tinyintunsigned":
            case "mediumint":
            case "mediumintunsigned":
            case "integer":
            case "int":
                return MathUtil.getIntegerVal(obj);

            case "bigint":
            case "bigintunsigned":
            case "intunsigned":
            case "integerunsigned":
                return MathUtil.getLongVal(obj);

            case "boolean":
                return MathUtil.getBoolean(obj);

            case "blob":
                return MathUtil.getByte(obj);

            case "varchar":
            case "char":
            case "text":
                return MathUtil.getString(obj);

            case "real":
            case "float":
            case "realunsigned":
            case "floatunsigned":
                return MathUtil.getFloatVal(obj);

            case "double":
            case "doubleunsigned":
                return MathUtil.getDoubleVal(obj);

            case "decimal":
            case "decimalunsigned":
                return MathUtil.getBigDecimal(obj);

            case "date":
                return MathUtil.getDate(obj);

            case "timestamp":
            case "datetime":
                return MathUtil.getTimestamp(obj);
            default:
        }
        return obj;
    }
}
