package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapPartition {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> texts = env.generateSequence(0,20).setParallelism(4);
        texts.mapPartition(new MyMapParition()).print();
    }

    public static class MyMapParition implements MapPartitionFunction<Long,Long>{

        @Override
        public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
            Long c = 0L;
            for(Long s : values){
                c++;
            }
            out.collect(c);
        }
    }
}
