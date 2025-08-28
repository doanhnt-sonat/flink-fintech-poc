#!/usr/bin/env python3
"""
Flink Word Count Job in Python
Equivalent to the Java WordCountJob
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common import Types
from pyflink.datastream.functions import FlatMapFunction, MapFunction


class Tokenizer(FlatMapFunction):
    """Tokenizer function to split text into words"""
    
    def flat_map(self, value):
        words = value.lower().split()
        for word in words:
            # Filter out empty strings and punctuation
            cleaned_word = ''.join(c for c in word if c.isalnum())
            if cleaned_word:
                yield (cleaned_word, 1)


def main():
    """Main function to execute the Word Count job"""
    
    # Create streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Sample data
    lines = [
        "Apache Flink is a powerful stream processor",
        "Flink is great for both batch and stream", 
        "Stream processing with Flink is efficient"
    ]
    
    # Create data stream from collection
    text_stream = env.from_collection(lines, type_info=Types.STRING())
    
    # Apply word count transformation
    word_counts = text_stream \
        .flat_map(Tokenizer(), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .sum(1)
    
    # Print results
    word_counts.print()
    
    # Execute the job
    env.execute("Python Flink WordCount Job")


if __name__ == "__main__":
    main()
