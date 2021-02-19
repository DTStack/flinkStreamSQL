package com.dtstack.flink.sql.util;

import org.junit.Assert;
import org.junit.Test;

public class ByteUtilsTest {

    @Test
    public void testToBoolean(){
        byte[] content = new byte[1];
        content[0] = 1;
        Assert.assertEquals(ByteUtils.toBoolean(content), true);
    }

    @Test
    public void testToString(){
        byte[] content = "test".getBytes();
        Assert.assertEquals(ByteUtils.byteToString(content), "test");
    }

    @Test
    public void testToShort4(){
        short content = 3;
        ByteUtils.shortToByte4(content);
    }

    @Test
    public void testByteToShort(){
        byte[] content = ByteUtils.shortToByte4((short)1);
        Assert.assertEquals(ByteUtils.byte2ToShort(content), new Short((short)1));
    }

    @Test
    public void testIntToByte(){
        ByteUtils.int2Bytes(2);
    }

    @Test
    public void testByteToInt(){
        byte[] content = ByteUtils.int2Bytes(2);
        Assert.assertEquals(ByteUtils.byte4ToInt(content), 2);
    }

    @Test
    public void testLong2Byte(){
        ByteUtils.long2Bytes(2L);
    }

    @Test
    public void testBytes2Long(){
        byte[] content = ByteUtils.long2Bytes(2l);
        Assert.assertEquals(ByteUtils.bytes2Long(content), 2l);
    }

    @Test
    public void testBytes2Byte(){
        byte[] content = ByteUtils.long2Bytes(2l);
       ByteUtils.bytes2Byte(content);
    }

    @Test
    public void testFloat2Array(){
        byte[] content = ByteUtils.float2ByteArray(2);
    }
    @Test
    public void testByte2Float(){
        byte[] content = ByteUtils.float2ByteArray(2);
        ByteUtils.bytes2Float(content);
    }
    @Test
    public void testDouble2Byte(){
        byte[] content = ByteUtils.double2Bytes(2);
    }
    @Test
    public void testByte2Double(){
        byte[] content = ByteUtils.double2Bytes(2);
        ByteUtils.bytes2Double(content);
    }
}
