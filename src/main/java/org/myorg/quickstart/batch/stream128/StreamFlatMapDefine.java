package org.myorg.quickstart.batch.stream128;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

//自定义转换函数
public class StreamFlatMapDefine {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("localhost",9999);

        text.flatMap(new MyFlatMapFunction()).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);

        env.execute("StreamingFlatMap");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            if(value.length()>0){
                String[] tokens = value.toLowerCase().split(" ");
                for(String token : tokens){
                    collector.collect(new Tuple2<>(token,1));
                }
            }
        }
    }
}
