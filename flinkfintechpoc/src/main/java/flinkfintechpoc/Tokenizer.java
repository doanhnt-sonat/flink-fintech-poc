package flinkfintechpoc;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

  @Override
  public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) {
    // Normalize and split the line into words
    String[] tokens = value.toLowerCase().split("\\W+");
    // Emit the words and their count
    for (String token : tokens) {
      if (!token.isEmpty()) {
        collector.collect(new Tuple2<>(token, 1));
      }
    }
  }
}