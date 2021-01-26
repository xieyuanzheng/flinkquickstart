package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

public class Iterator {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int maxIteration = 10;
        DataSet<Long> input = env.generateSequence(1,5);
        //启动迭代
        IterativeDataSet<Long> initial = input.iterate(maxIteration);

        DataSet<Long> mapData = initial.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value+1;
            }
        });

        DataSet<Long> result = initial.closeWith(mapData);

        result.print();;
    }
}
