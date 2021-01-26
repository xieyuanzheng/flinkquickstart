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

public class FullOuterJoin {

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
                text1.fullOuterJoin(text2)
                .where(0). //左表 第一个字段
                equalTo(0)
                .with(new MyJoinFunction());
        joinedData.print();
    }

    public static class MyJoinFunction implements JoinFunction<Tuple2<Integer,String>,Tuple2<Integer,String>,UserInfo>{

        @Override
        public UserInfo join(Tuple2<Integer,String> first, Tuple2<Integer,String> second) throws Exception {
            UserInfo userInfo = new UserInfo();
            if(first==null){
                userInfo.setUserId(0);
                userInfo.setUserName("xiaoming");
            }else {
                userInfo.setUserId(first.f0);
                userInfo.setUserName(first.f1);
            }if(second==null){
                userInfo.setUserId2(0);
                userInfo.setAddress("china");
            }else {
                userInfo.setUserId2(second.f0);
                userInfo.setAddress(second.f1);
            }
            return userInfo;
        }
    }

    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserInfo{
        private Integer userId;
        private String userName;
        private Integer userId2;
        private String address;

        public UserInfo(Integer userId) {
            this.userId = userId;
        }
    }
}
