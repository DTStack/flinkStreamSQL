package com.dtstack.flink.sql.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  API调用结果返回
 * Date: 2020/2/24
 * Company: www.dtstack.com
 * @author maqi
 */
public class ApiResult {

    private static final Logger LOG = LoggerFactory.getLogger(ApiResult.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final Integer FAIL = 0;
    public static final Integer SUCCESS = 1;

    private int code;
    private long space;
    private Object data;
    private String errorMsg;

    public ApiResult() {
    }

    public static String createSuccessResultJsonStr(String message,long space) {
        ApiResult apiResult = createSuccessResult(SUCCESS, message);
        apiResult.setSpace(space);
        String result;
        try {
            result = OBJECT_MAPPER.writeValueAsString(apiResult);
        } catch (Exception e) {
            LOG.error("", e);
            result = "code:" + SUCCESS + ",message:" + message;
        }
        return result;
    }

    public static ApiResult createSuccessResult(int code, String message) {
        ApiResult apiResult = new ApiResult();
        apiResult.setCode(code);
        apiResult.setData(message);
        return apiResult;
    }

    public static String createErrorResultJsonStr(String message) {
        ApiResult apiResult = createErrorResult(message, FAIL);
        String result;
        try {
            result = OBJECT_MAPPER.writeValueAsString(apiResult);
        } catch (Exception e) {
            LOG.error("", e);
            result = "code:" + FAIL + ",message:" + message;
        }
        return result;
    }

    public static ApiResult createErrorResult(String errMsg, int code) {
        ApiResult apiResult = new ApiResult();
        apiResult.setCode(code);
        apiResult.setErrorMsg(errMsg);
        return apiResult;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public long getSpace() {
        return space;
    }

    public void setSpace(long space) {
        this.space = space;
    }
}
