package org.myorg.quickstart.batch.stream128;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamSocket {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        socketFunction(env);
        env.execute("StreamSocket");
    }

    public static void socketFunction(StreamExecutionEnvironment env){
        DataStreamSource data = env.socketTextStream("localhost", 9999);
        data.print().setParallelism(1);

    }

}
