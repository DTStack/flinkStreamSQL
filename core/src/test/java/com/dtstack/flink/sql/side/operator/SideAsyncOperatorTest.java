package com.dtstack.flink.sql.side.operator;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.util.PluginUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PluginUtil.class, ClassLoaderManager.class, AsyncDataStream.class})
public class SideAsyncOperatorTest {

    @Test
    public void getSideJoinDataStream() throws Exception {
        DataStream inputStream = mock(DataStream.class);
        String sideType = "redis";
        String sqlRootDir = "/";
        RowTypeInfo rowTypeInfo = mock(RowTypeInfo.class);
        JoinInfo joinInfo = mock(JoinInfo.class);

        List<FieldInfo> outFieldInfoList = Lists.newArrayList();
        AbstractSideTableInfo sideTableInfo = mock(AbstractSideTableInfo.class);
        when(sideTableInfo.getParallelism()).thenReturn(1);

        PowerMockito.mockStatic(PluginUtil.class);
        when(PluginUtil.getJarFileDirPath(anyString(), anyString(),anyString())).thenReturn("/");

        PowerMockito.mockStatic(ClassLoaderManager.class);
        BaseAsyncReqRow baseAsyncReqRow = mock(BaseAsyncReqRow.class);
        when(ClassLoaderManager.newInstance(anyString(), anyObject())).thenReturn(baseAsyncReqRow);

        SingleOutputStreamOperator dataStream = mock(SingleOutputStreamOperator.class);
        PowerMockito.mockStatic(AsyncDataStream.class);
        when(AsyncDataStream.unorderedWait(anyObject(), anyObject(), anyLong(), anyObject(), anyInt())).thenReturn(dataStream);
        when(AsyncDataStream.orderedWait(anyObject(), anyObject(), anyLong(), anyObject(), anyInt())).thenReturn(dataStream);

        SideAsyncOperator.getSideJoinDataStream(inputStream, sideType, sqlRootDir, rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo, "");

        when(sideTableInfo.getCacheMode()).thenReturn("ORDERED");
        SideAsyncOperator.getSideJoinDataStream(inputStream, sideType, sqlRootDir, rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo, "");




    }

}
