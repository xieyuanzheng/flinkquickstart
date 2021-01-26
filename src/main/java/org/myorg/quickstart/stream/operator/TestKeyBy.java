package org.myorg.quickstart.stream.operator;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TestKeyBy {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple4<String,String,String,Integer>> input = env.fromElements(TRANSCRIPT);
        System.out.println("----------" + input.getParallelism());

       /* KeyedStream<Tuple4<String,String,String,Integer>,Tuple> keyedStream = input.keyBy("f0");
        keyedStream.max("f3").print();*/

        KeyedStream<Tuple4<String,String,String,Integer>,Tuple> keyedStream = input.keyBy("f0");
        keyedStream.maxBy("f3").print();


       /* KeyedStream<Tuple4<String,String,String,Integer>,String> keyedStream = input.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple4<String, String, String, Integer> value) throws Exception {
                return value.f2;
            }
        });
        keyedStream.print();*/


        env.execute();
    }

    public static final Tuple4[] TRANSCRIPT = new Tuple4[]{
            Tuple4.of("class1","张三","语文",100),
            Tuple4.of("class1","李四","语文",78),
            Tuple4.of("class1","王五","语文",99),
            Tuple4.of("class2","赵六","语文",81),
            Tuple4.of("class2","钱七","语文",59),
            Tuple4.of("class2","马二","语文",97)
    };
}
