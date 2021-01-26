package org.myorg.quickstart.batch.batch111;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class JoinWith {
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

        DataSet<UserInfo> joinedData =
                text1.join(text2)
                .where(0). //左表 第一个字段
                 equalTo(0)//右表 第一个字段
                .with(new MyJoinFunction());
        joinedData.print();
    }

    public static class MyJoinFunction implements JoinFunction<Tuple2<Integer,String>,Tuple2<Integer,String>,UserInfo>{

        @Override
        public UserInfo join(Tuple2<Integer,String> first, Tuple2<Integer,String> second) throws Exception {
            return new UserInfo(first.f0,first.f1,second.f1);
        }
    }

    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserInfo{
        private Integer userId;
        private String userName;
        private String address;
    }
}
