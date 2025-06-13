package flinkfintechpoc.jobs;
import flinkfintechpoc.Tokenizer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class WordCountJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    List<String> lines = Arrays.asList(
        "Apache Flink is a powerful stream processor",
        "Flink is great for both batch and stream",
        "Stream processing with Flink is efficient"
    );

    DataStream<String> text = env.fromCollection(lines, TypeInformation.of(String.class));

    DataStream<Tuple2<String, Integer>> counts = text
        .flatMap(new Tokenizer())
        .keyBy(tuple -> tuple.f0)
        .sum(1);

    counts.print();
    env.execute("Flink 2.0 WordCount with Static Data");
  }
}
