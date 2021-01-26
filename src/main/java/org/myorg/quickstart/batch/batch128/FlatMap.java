package org.myorg.quickstart.batch.batch128;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.net.URL;


public class FlatMap {
    public static void main(String[] args) throws Exception{
        String input_path = "/*/work/project/java/flinkquickstart/src/main/java/org/myorg/quickstart/batch/batch128/input.2.txt";
        String input = "/*/work/project/java/flinkquickstart/src/main/resources/1.2.txt";
        ClassLoader classLoader = FlatMap.class.getClassLoader();
        URL resource = classLoader.getResource("1.txt");
        String path = resource.getPath();

        //Step1: create environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Step2: read textfile
        DataSource<String> text = env.readTextFile(path);

        //Step3: transform
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if(value.length()>0){
                    String[] tokens = value.toLowerCase().split(" ");
                    for(String token : tokens){
                        collector.collect(new Tuple2<String, Integer>(token,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();


        //Step4: execution
    }
}
