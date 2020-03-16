package com.ververica.flinktraining.zhch;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WordCountSideOutput {
	static final OutputTag<String> shortWordsTag = new OutputTag<String>("short") {};

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = env
			.addSource(new SocketTextStreamFunction("localhost", 9999, "\n", -1))
			.process(new Tokenizer());

		DataStream<String> shortWords = tokenized.getSideOutput(shortWordsTag);
		shortWords.print();

		DataStream<Tuple2<String, Integer>> wordCounts = tokenized.keyBy(0).sum(1);
		wordCounts.print();

		env.execute("Streaming WordCount");
	}
	
	public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {

		@Override
		public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			for (String token : tokens) {
				if (token.length() < 5) {
					// send short words to a side output
					ctx.output(shortWordsTag, token);
				} else if (token.length() > 0) {
					// emit the pair
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}
