package org.myorg.quickstart.batch.batch128;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class SinkDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> list = new ArrayList<Integer>();
        for(int i=0;i<=10;i++){
            list.add(i);
        }

        DataSource<Integer> text = env.fromCollection(list);
        String path = "/*/Downloads/ouput";
        text.writeAsText(path, FileSystem.WriteMode.OVERWRITE);
        env.execute("SinkDemo");
    }
}
