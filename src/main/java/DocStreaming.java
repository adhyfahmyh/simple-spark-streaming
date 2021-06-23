import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.*;

public class DocStreaming {
    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) throws InterruptedException {

        // Configure SparkStreamingContext
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("StreamingFromDocumentaion");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // Receive streaming data
        JavaReceiverInputDStream<String> lines =
                javaStreamingContext
                        .socketTextStream(HOST, PORT);
        Logger.getRootLogger().setLevel(Level.ERROR);

        // processing the data
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

//        lines.print();
        wordCounts.print();

        // Execute spark
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }


}
