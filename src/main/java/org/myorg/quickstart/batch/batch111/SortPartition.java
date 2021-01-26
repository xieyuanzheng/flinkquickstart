package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


public class SortPartition {
    public static void main(String[] args) throws Exception{
        /**
         * partition内排序
         */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer,String>> data = new ArrayList<>();
        data.add(Tuple2.of(2,"zs"));
        data.add(Tuple2.of(4,"ls"));
        data.add(Tuple2.of(3,"ww"));
        data.add(Tuple2.of(1,"xw"));
        data.add(Tuple2.of(1,"aw"));
        data.add(Tuple2.of(1,"mw"));

        DataSource<Tuple2<Integer,String>> text = env.fromCollection(data).setParallelism(2);
        //先根据index=0升序，如果一样，则对index=1进行降序
        text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING)
                .mapPartition(new MyMapPartition()).print();
        System.out.println("-----同一个id被分到同一个分区------");
        text.partitionByHash(0).mapPartition(new MyMapPartition()).print();

    }

    public static class MyMapPartition extends RichMapPartitionFunction<Tuple2<Integer,String>,Tuple2<Integer,String>>{

        @Override
        public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
            for(Tuple2<Integer, String> item : values){
                System.out.println("当前subtaskIndex : "+getRuntimeContext().getIndexOfThisSubtask()
                + ", " + item);
            }
        }
    }
}
