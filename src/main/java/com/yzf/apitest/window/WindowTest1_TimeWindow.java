package com.yzf.apitest.window;

import com.yzf.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：zhangyu
 * @date ：Created in 2021/7/13 16:46
 * @description：
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("E:\\workspace\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 从socket文本流读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost",7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗测试

        // 1.增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy("id")
                // 会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)));
                // 滚动时间窗口
                .timeWindow(Time.seconds(2))
                // 滑动时间窗口
//                .timeWindow(Time.seconds(15), Time.seconds(5));
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        // 2. 全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> outputStreamOperator = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String,Long,Integer>> collector) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = timeWindow.getEnd();
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(id,windowEnd,count));
                    }
                });


//        resultStream.print();
        outputStreamOperator.print();

        env.execute();
    }
}
