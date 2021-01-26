package org.myorg.quickstart.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class FromCollection {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List list = new ArrayList();
        list.add(1);
        list.add(2);
        list.add(3);
        DataStreamSource collection = env.fromCollection(list);
        collection.print();

        env.execute("FromCollection");
    }
}
