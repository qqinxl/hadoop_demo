package org.shin.hadoop.demo.wordcount;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountTest {

    /**
     * @param args
     */
    public static void main(final String[] args) {

        try {
            // TODO 自動生成されたメソッド・スタブ
            Configuration conf = new Configuration();
            Job job = new Job(conf, "wordcount demo");
            job.setJarByClass(WordCountTest.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(WordCountMapper.class);
            job.setCombinerClass(WordCountReducer.class);
            job.setReducerClass(WordCountReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 実行を開始して完了するまで待つ
            boolean success = job.waitForCompletion(true);
            System.out.println(success);
            // 実行を開始して(完了を待たずに)すぐ戻ってくる
            // job.submit();

            // while (!job.isComplete());
            // boolean success = job.isSuccessful();
            // System.out.println(success);
        } catch (IOException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
        }

    }

}
