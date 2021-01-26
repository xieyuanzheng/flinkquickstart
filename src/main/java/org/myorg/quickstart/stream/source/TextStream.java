package org.myorg.quickstart.stream.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

public class TextStream {
    public static void main(String[] args) throws Exception{
        //获取text文件
        ClassLoader classLoader = TextStream.class.getClassLoader();
        URL url = classLoader.getResource("1.txt");
        String path = url.getPath();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.readTextFile(path);

        text.print();
        env.execute("TextStream");

    }
}
