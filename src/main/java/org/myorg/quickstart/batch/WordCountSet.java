package org.myorg.quickstart.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public class WordCountSet {
    public static void main(String[] args) throws Exception{
        //获取flink运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //加载数据源到DataSet
        DataSet<String> text = env.readTextFile("/*/work/project/java/flinkquickstart/src/main/resources/1.2.txt");
        //可以理解DataSet为一个RDD
        //groupby（0）意思是以第一个为key进行分组，sum（1）是对元组第二个位置数据进行累加
        DataSet<Tuple2<String,Integer>> count = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //即从文本中读取到一行字符串，按空格分割后得到数组
                for(String word : value.toLowerCase().split("\\s+")){
                    if(word.length() > 0){
                        //初始化每一个单词，保存为元组对象
                        //out.collect(Tuple2.of(word,1));
                        out.collect(new Tuple2<>(word,1));
                    }
                }
            }
        }).groupBy(0)//0表示Tuple2<String,Integer>中的第一个元素，即分割后的单词
          .aggregate(Aggregations.SUM,1); //同理，1表示Tuple2<String,Integer> 中的第二个元素，即出现次数

        count.print();

        /*List<Tuple2<String,Integer>> list = count.collect();
        for(Tuple2<String,Integer> tuple2 : list){
            System.out.println(tuple2.f0 + " : " + tuple2.f1);
        }*/
    }
}
