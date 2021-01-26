package org.myorg.quickstart.batch.batch128;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.net.URL;

public class FlatMap1 {
    public static void main(String[] args) {
        ClassLoader classLoader = FlatMap1.class.getClassLoader();
        URL resource = classLoader.getResource("1.txt");
        String path = resource.getPath();

        //Step1: create environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Step2: read the file
        DataSource<String> text = env.readTextFile(path);

        //Step3: transform
        FlatMapOperator flatMapOperator = text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if(s.length()>0){
                    String[] tokens = s.toLowerCase().split(" ");
                    for(String token : tokens){
                        collector.collect(new Tuple2<>(token,1));
                    }
                }
            }
        });

        //Step4: execute
        try {
            flatMapOperator.groupBy(0).sum(1).print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
