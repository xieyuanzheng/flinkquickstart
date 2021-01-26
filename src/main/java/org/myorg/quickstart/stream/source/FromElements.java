package org.myorg.quickstart.stream.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromElements {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> elements = env.fromElements("hello","world","flink");
        elements.print();
        env.execute("FromElements");
    }
}
