package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class IteratorPI {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int maxIterator = 10;
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(maxIterator);
        DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return value + ((x*x + y*y<=1) ? 1 : 0);
            }
        });

        DataSet<Integer> count = initial.closeWith(iteration);
        count.print();

        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer value) throws Exception {
                System.out.println(value);
                System.out.println((double)maxIterator);
                System.out.println(value / (double)maxIterator * 4);
                return value / (double)maxIterator * 4;
            }
        }).print();
    }
}
