package com.yzf.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ：zhangyu
 * @date ：Created in 2021/6/9 11:36
 * @description：批处理式 wordCount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "E:\\workspace\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成(word,1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) //按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和
        resultSet.print();
    }
    // 自定义类，实现FlatMapFunction接口

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按空格进行分词
            String[] words = s.split(" ");

            //遍历所有word，包成二元组输出
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}
