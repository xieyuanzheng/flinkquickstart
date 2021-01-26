package org.myorg.quickstart.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GenerateSequence {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> generate = env.generateSequence(1,10);

        generate.print();
        env.execute("GenerateSequence");
    }
}
