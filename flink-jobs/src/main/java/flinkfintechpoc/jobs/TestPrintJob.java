package flinkfintechpoc.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestPrintJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env
            .fromSequence(0, Long.MAX_VALUE)
            .map(new MapFunction<Long, String>() {
                @Override
                public String map(Long value) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "test";
                }
            })
            .name("Tick 1s -> test");

        stream.print().name("Print Test");

        env.execute("Simple Test Print Job");
    }
}


