package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class Cross {
    /**
     * 笛卡尔积
     */
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //用户姓名列表
        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
        data1.add(Tuple2.of(1,"zs"));
        data1.add(Tuple2.of(2,"ls"));
        data1.add(Tuple2.of(3,"ww"));
        //用户城市列表
        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(Tuple2.of(1,"beijing"));
        data2.add(Tuple2.of(2,"shanghai"));
        DataSet<Tuple2<Integer,String>> text1 = env.fromCollection(data1);
        DataSet<Tuple2<Integer,String>> text2 = env.fromCollection(data2);

        DataSet<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>> joinedData =
                text1.cross(text2);
        joinedData.print();
    }
}
