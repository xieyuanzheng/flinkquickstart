package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class Distinct {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Long,String,Integer>> inputs = env.fromElements(
                Tuple3.of(1L,"zhangshan",28),
                Tuple3.of(3L,"lisi",34),
                Tuple3.of(3L,"wangwu",23),
                Tuple3.of(3L,"lisi",34),
                Tuple3.of(3L,"maqi",25)
        );
        inputs.distinct().print();
    }
}
