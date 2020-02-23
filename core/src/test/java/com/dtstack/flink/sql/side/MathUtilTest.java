package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.util.MathUtil;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * Reason:
 * Date: 2020/1/6
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MathUtilTest {

    @Test
    public void testGetDoubleVal(){
        Double doubleVal = 1.1D;
        Float floatVal = 1.1F;
        Integer intVal = 1;
        String strVal = "1";
        BigDecimal bigDecimalVal = new BigDecimal(1.1);

        MathUtil.getDoubleVal(doubleVal);
        MathUtil.getDoubleVal(floatVal);
        MathUtil.getDoubleVal(intVal);
        MathUtil.getDoubleVal(strVal);
        MathUtil.getDoubleVal(bigDecimalVal);
    }
}
