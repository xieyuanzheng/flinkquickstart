package org.myorg.quickstart.stream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class CustomSource {
    private static final int BOUND = 100;
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer,Integer>> source = env.addSource(new RichParallelSampleSource());
        source.print();
        env.execute("CustomSource");
    }

    //非并行，并行度为1
    public static class SimpleSource implements SourceFunction<Tuple2<Integer,Integer>>{
        private static final long serialVersionUID = 1L;
        Random rnd = new Random();
        private volatile boolean isRunning = true;
        private int count = 0;
        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (isRunning && count<BOUND){
                int first = rnd.nextInt(BOUND/2 - 1) + 1;
                int second = rnd.nextInt(BOUND/2 - 1) + 1;
                ctx.collect(new Tuple2<>(first,second));
                count++;
                Thread.sleep(1000);
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }


    public static class ParallelSimpleSource implements ParallelSourceFunction<Tuple2<Integer,Integer>>{

        private static final long serialVersionUID = 1L;
        Random rnd = new Random();
        private volatile boolean isRunning = true;
        private int count = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (isRunning && count<2){
                int first = rnd.nextInt(BOUND/2 - 1) + 1;
                int second = rnd.nextInt(BOUND/2 - 1) + 1;
                ctx.collect(new Tuple2<>(first,second));
                count++;
                Thread.sleep(500);
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class RichSampleSource extends RichSourceFunction<Tuple2<Integer, Integer>> {


        private static final long serialVersionUID = 1L;
        Random rnd = new Random();
        private volatile boolean isRunning = true;
        private int count = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (isRunning && count<2){
                int first = rnd.nextInt(BOUND/2 - 1) + 1;
                int second = rnd.nextInt(BOUND/2 - 1) + 1;
                ctx.collect(new Tuple2<>(first,second));
                count++;
                Thread.sleep(500);
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class RichParallelSampleSource extends RichParallelSourceFunction<Tuple2<Integer, Integer>> {


        private static final long serialVersionUID = 1L;
        Random rnd = new Random();
        private volatile boolean isRunning = true;
        private int count = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (isRunning && count<2){
                int first = rnd.nextInt(BOUND/2 - 1) + 1;
                int second = rnd.nextInt(BOUND/2 - 1) + 1;
                ctx.collect(new Tuple2<>(first,second));
                count++;
                Thread.sleep(500);
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
