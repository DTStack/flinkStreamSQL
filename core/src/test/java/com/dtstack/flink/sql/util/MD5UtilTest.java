package com.dtstack.flink.sql.util;

import org.junit.Assert;
import org.junit.Test;

public class MD5UtilTest {

    @Test
    public void testGetMD5String(){
        Assert.assertNotEquals(MD5Utils.getMD5String("aaaaa"), "d41d8cd98f00b204e9800998ecf8427e");
    }

}
