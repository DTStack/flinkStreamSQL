package com.dtstack.flink.sql.side;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.List;

public class WatermarkTest {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketWatermarkTest <hostname> <port>");
            return;
        }

        String hostName = args[0];
        Integer port = Integer.valueOf(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        DataStreamSource<String> input = env.socketTextStream(hostName, port);

        DataStream inputMap = input.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split("\\W+");
                        String code = arr[0];
                        Long time = Long.valueOf(arr[1]);
                        return new Tuple2(code, time);
                    }
                }
        );

        DataStream watermarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            Long maxOutOfOrderness = 10000L; //最大允许的乱序时间是10s

            Watermark watermark = null;

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public Watermark getCurrentWatermark() {
                watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                return watermark;
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                Long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("timestamp:" + element.f0 + "," + element.f1 + "|" + format.format(element.f1) + "," + currentMaxTimestamp + "|" + format.format(currentMaxTimestamp) + "," + watermark);
                return timestamp;
            }

        });

        DataStream window = watermarkStream
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //.countWindow(100)
                .apply(new WindowFunctionTest());

        window.print();

        env.execute("watermark test");

    }
}

class WindowFunctionTest implements WindowFunction<Tuple2<String, Long>, Tuple6<String, Integer, String, String, String, String>, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple6<String, Integer, String, String, String, String>> out) throws Exception {
        List<Tuple2<String, Long>> list = (List) input;
        list.sort((Tuple2<String, Long> t1, Tuple2<String, Long> t2) -> t1.f1.compareTo(t2.f1));
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        out.collect(new Tuple6<>(key, list.size(), format.format(list.get(0).f1), format.format(list.get(list.size() - 1).f1), format.format(window.getStart()), format.format(window.getEnd())));
    }

}