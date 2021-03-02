package com.dtstack.flink.sql.util;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SqlKind.class})
public class ParseUtilTest {


    @Test
    public void parseAnd(){
        SqlBasicCall sqlNode = mock(SqlBasicCall.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.AND);

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.SELECT);

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        sqlNodeList.add(sqlBasicCall);
        sqlNodeList.add(sqlBasicCall);

        when(sqlNode.getOperandList()).thenReturn(sqlNodeList);
        when(sqlNode.getOperands()).thenReturn(sqlNodeList.toArray(new SqlNode[2]));

        ParseUtils.parseAnd(sqlNode, Lists.newArrayList());

    }

    @Test
    public void parseJoinCompareOperate(){
        SqlBasicCall sqlNode = mock(SqlBasicCall.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.AND);

        SqlBasicCall sqlBasicCall = mock(SqlBasicCall.class);
        when(sqlBasicCall.getKind()).thenReturn(SqlKind.SELECT);
        when(sqlBasicCall.toString()).thenReturn("s");

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        sqlNodeList.add(sqlBasicCall);

        when(sqlNode.getOperandList()).thenReturn(sqlNodeList);
        when(sqlNode.getOperands()).thenReturn(sqlNodeList.toArray(new SqlNode[2]));
        ParseUtils.parseJoinCompareOperate(sqlNode, Lists.newArrayList());
    }

    @Test
    public void transformNotEqualsOperator(){
        SqlKind sqlKind = mock(SqlKind.class);
        when(sqlKind.toString()).thenReturn("NOT_EQUALS");
        ParseUtils.transformNotEqualsOperator(sqlKind);
    }

    @Test
    public void dealDuplicateFieldName(){
        HashBasedTable<String, String, String> mappingTable = HashBasedTable.<String, String, String>create();
        mappingTable.put("a", "b", "c");
        ParseUtils.dealDuplicateFieldName(mappingTable, "a");
    }


    @Test
    public void dealDuplicateFieldNameWithBi(){
        HashBiMap<String, String> refFieldMap = HashBiMap.<String, String>create();
        refFieldMap.put("a", "b");
        ParseUtils.dealDuplicateFieldName(refFieldMap, "a");
    }
    @Test
    public void dealDuplicateFieldNameWithHash(){
        Map<String, String> refFieldMap = new HashMap<String, String>();
        refFieldMap.put("a", "b");
        ParseUtils.dealDuplicateFieldName(refFieldMap, "a");
    }

    @Test
    public void suffixWithChar(){
        ParseUtils.suffixWithChar("a", 'a', 1);
    }



}
