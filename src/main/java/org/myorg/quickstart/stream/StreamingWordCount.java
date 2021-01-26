package org.myorg.quickstart.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWordCount {
    public static void main(String[] args) throws Exception{
        //创建flink执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //接收socket的输入流
        //使用本地9000端口，如端口被占用可换一个端口
        DataStream<String> text = env.socketTextStream("127.0.0.1",9000,"\n");

        //使用Flink算子对输入流的文本进行操作
        //按空格切词、计数、分组、设置时间窗口、聚合
        DataStream<Tuple2<String,Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for(String word : value.split("\\s+")){
                            out.collect(Tuple2.of(word,1));
                        }
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        //单线程打印结果
        windowCounts.print().setParallelism(1);

        env.execute("Socket windows WordCount");
    }
}
