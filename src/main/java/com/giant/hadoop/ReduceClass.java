package com.giant.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {

        long count = 0;
        for (LongWritable num : values) {
            count += num.get();
        }
        context.write(key, new LongWritable(count));
    }
}
