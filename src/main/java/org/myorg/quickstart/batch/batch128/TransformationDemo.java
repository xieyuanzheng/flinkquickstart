package org.myorg.quickstart.batch.batch128;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;
import java.util.List;

public class TransformationDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        filterFunction(env);

    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();
        for(int i=0;i<=10;i++){
            list.add(i);
        }
        DataSource<Integer> text = env.fromCollection(list);

        text.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {
                return input>5;
            }
        }).print();
    }

    public static void mapFuntion(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();
        for(int i=0;i<=10;i++){
            list.add(i);
        }
        DataSource<Integer> text = env.fromCollection(list);

        text.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).print();
    }
}
