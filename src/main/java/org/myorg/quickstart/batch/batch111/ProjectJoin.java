package org.myorg.quickstart.batch.batch111;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class ProjectJoin {
    /**
     * 默认是等值连接，就是inner join
     */
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //用户姓名列表
        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
        data1.add(Tuple2.of(1,"zs"));
        data1.add(Tuple2.of(2,"ls"));
        data1.add(Tuple2.of(3,"ww"));
        data1.add(Tuple2.of(3,"cc"));
        data1.add(Tuple2.of(4,"zs"));
        //用户城市列表
        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(Tuple2.of(1,"beijing"));
        data2.add(Tuple2.of(2,"shanghai"));
        data2.add(Tuple2.of(3,"guangzhou"));
        data2.add(Tuple2.of(5,"shenzhen"));
        DataSet<Tuple2<Integer,String>> text1 = env.fromCollection(data1);
        DataSet<Tuple2<Integer,String>> text2 = env.fromCollection(data2);

        DataSet<Tuple3<Integer,String,String>> joinedData =
                text1.join(text2)
                .where(0). //左表 第一个字段
                 equalTo(0).projectFirst(0,1).projectSecond(1);//右表 第一个字段
        joinedData.print();
    }
}
