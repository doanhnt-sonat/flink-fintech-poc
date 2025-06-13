package flinkfintechpoc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>, Serializable {
  @Override
  public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
    for (String word : value.toLowerCase().split("\\W+")) {
      if (!word.isEmpty()) {
        out.collect(new Tuple2<>(word, 1));
      }
    }
  }
}