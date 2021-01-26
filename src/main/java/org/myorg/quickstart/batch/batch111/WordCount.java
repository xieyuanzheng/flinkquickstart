package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements("how are you","I am fine, and you");
        DataSet<Tuple2<String,Integer>> wc = text.flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1);
        wc.print();
    }


    private static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
            String[] values = value.split(" ");
            for(String s : values){
                out.collect(new Tuple2<>(s, 1));
            }

        }
    }
}
