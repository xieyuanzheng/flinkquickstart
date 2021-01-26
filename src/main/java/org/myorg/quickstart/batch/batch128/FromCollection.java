package org.myorg.quickstart.batch.batch128;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class FromCollection {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        getCollection(env);
    }

    public static void getCollection(ExecutionEnvironment env){
        List<Integer> list = new ArrayList<Integer>();
        for(int i=0;i<=10;i++){
            list.add(i);
        }
        try {
            env.fromCollection(list).print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
