package org.myorg.quickstart.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

public class WordCountSet2 {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile("/*/work/project/java/flinkquickstart/src/main/resources/1.2.txt");

        //demo1中的aggregate聚合函数被替换成了reduce，这是因为aggregate函数只接受int来表示field。同时，groupby（0）也相应改成用groupby（"word"）直接指定字段
        DataSet<WordWithCount> count = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] tokens = value.toLowerCase().split("\\s+");
                for (String word : tokens){
                    if(word.length() > 0){
                        out.collect(new WordWithCount(word,1));
                    }
                }
            }
        }).groupBy("word")//直接指定字段名称
        .reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount wc, WordWithCount t1) throws Exception {
                return new WordWithCount(wc.word,wc.count+t1.count);
            }
        });

        //count.print();

        List<WordWithCount> list = count.collect();
        for(WordWithCount w : list){
            System.out.println(w.toString());
        }
    }

    //pojo
    /*请注意，如果你的pojo demo 运行失败，你可能需要做以下检查工作：
    1、pojo 有没有声明为public，如果是内部类必须是static的
    2.txt、有没有为pojo创建一个无参的构造函数
    3、有没有声明pojo的字段为public，或者生成public的get，set方法
    4、必须使用Flink 支持的数据类型*/
    public static class WordWithCount{
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
