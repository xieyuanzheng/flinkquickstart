package org.myorg.quickstart.stream.source;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class TestSinkToMySQL {
    public static final String TOPIC = "student";

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "IP:9092");
        props.put("zookeeper.connect", "IP:2181");
        props.put("group.id", "group1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer010<String> myconsumer = new FlinkKafkaConsumer010<String>(TOPIC,new SimpleStringSchema(),props);
        SingleOutputStreamOperator student = env.addSource(myconsumer).setParallelism(1)
                .map(string -> JSON.parseObject(string,Student.class));//Fastjson 解析字符串成 student 对象
        student.addSink(new SinkToMySQL());
        env.execute("Flink add sink");
    }
}
