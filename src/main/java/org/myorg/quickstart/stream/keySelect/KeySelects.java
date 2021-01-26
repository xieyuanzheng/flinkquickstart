package org.myorg.quickstart.stream.keySelect;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class KeySelects {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        numberSelect(env);

    }

    /**
     * keyBy(param)中根据来源的次序
     * @param env
     * @throws Exception
     */
    public static void numberSelect(StreamExecutionEnvironment env) throws Exception{
        DataStream<String> text = env.socketTextStream("localhost",9999);
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if(input.length()>0){
                    for(String token : input.split("\\s+")){
                        collector.collect(new Tuple2<>(token,1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();

        env.execute("numberSelect");
    }

    /**
     * keyBy(param)中根据来源的次序的字符串顺序，如f0、f1、f2.....
     * @param env
     * @throws Exception
     */
    public static void fieldSelect(StreamExecutionEnvironment env) throws Exception{
        DataStream<String> text = env.socketTextStream("localhost",9999);
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if(input.length()>0){
                    for(String token : input.split("\\s+")){
                        collector.collect(new Tuple2<>(token,1));
                    }
                }
            }
        }).keyBy("f0").timeWindow(Time.seconds(5)).sum(1).print();

        env.execute("fieldSelect");
    }

    /**
     * keyBy(param)中根据来源的POJO类来获取
     * @param env
     * @throws Exception
     */
    public static void pojoFiledSelect(StreamExecutionEnvironment env) throws Exception{
        DataStream<String> text = env.socketTextStream("localhost",9999);
        text.flatMap(new FlatMapFunction<String, WordCountData>() {
            @Override
            public void flatMap(String input, Collector<WordCountData> collector) throws Exception {
                if(input.length()>0){
                    for(String token : input.split("\\s+")){
                        collector.collect(new WordCountData(token,1));
                    }
                }
            }
        }).keyBy("name").timeWindow(Time.seconds(5)).sum("count").print().setParallelism(1);

        env.execute("pojoFiledSelect");
    }

    /**
     * keyBy(param1, param2)组合键
     * @param env
     * @throws Exception
     */
    public static void combinationSelect(StreamExecutionEnvironment env) throws Exception{
        DataStream<String> text = env.socketTextStream("localhost",9999);
        text.flatMap(new FlatMapFunction<String, Tuple3<String,Integer,Integer>>() {
            @Override
            public void flatMap(String input, Collector<Tuple3<String,Integer,Integer>> collector) throws Exception {
                if(input.length()>0){
                    for(String token : input.split("\\s+")){
                        collector.collect(new Tuple3<>(token,token.length(),1));
                    }
                }
            }
        }).keyBy(0,1).timeWindow(Time.seconds(5)).sum(2).print();

        env.execute("combinationSelect");
    }

    /**
     * keyBy(param)自定义实现KeySelect实现
     * @param env
     * @throws Exception
     */
    public static void customizeSelect(StreamExecutionEnvironment env) throws Exception{
        DataStream<String> text = env.socketTextStream("localhost",9999);
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if(input.length()>0){
                    for(String token : input.split("\\s+")){
                        collector.collect(new Tuple2<>(token,1));
                    }
                }
            }
        }).keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).timeWindow(Time.seconds(5)).sum(1).print();

        env.execute("numberSelect");
    }


}
