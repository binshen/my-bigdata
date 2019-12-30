package com.giant.crunch;

import com.giant.hadoop.WordCount;
import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;

// https://my.oschina.net/lwhmdj0823/blog/633782?p=1
public class ApacheCrunch {

    public static void main(String[] args) {
        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(WordCount.class);
        // Reference a given text file as a collection of Strings.
        PCollection<String> lines = pipeline.readTextFile("input/hello.txt");

        // Define a function that splits each line in a PCollection of Strings into a
        // PCollection made up of the individual words in the file.
        PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
            public void process(String line, Emitter<String> emitter) {
                for (String word : line.split("\\s+")) {
                    emitter.emit(word);
                }
            }
        }, Writables.strings()); // Indicates the serialization format

        // The count method applies a series of Crunch primitives and returns
        // a map of the top 20 unique words in the input PCollection to their counts.
        // We then read the results of the MapReduce jobs that performed the
        // computations into the client and write them to stdout.
        for (Pair<String, Long> wordCount : words.count().top(20).materialize()) {
            System.out.println(wordCount);
        }
    }
}
