package flinkfintechpoc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Step 2: Define data sources
		DataStream<String> text = env.fromElements(
				"apple",
				"banana apple",
				"cherry"
		);

		// Step 3: Define transformations
		DataStream<Tuple2<String, Integer>> wordCounts = text
				.flatMap(new Tokenizer())
				.keyBy(tuple -> tuple.f0)
				.sum(1);

		// Step 4: Define sinks
		wordCounts.print();

		// Step 5: Execute the program
		env.execute("Flink WordCount Example");
	}
}
