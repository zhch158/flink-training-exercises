package com.ververica.flinktraining.zhch;

import java.util.Iterator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;

public class ControlConnectedStream {
	public static void main(String[] args) throws Exception {
	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> control = env.fromElements("DROP", "IGNORE", "zhch").keyBy(x -> x);
		DataStream<String> streamOfWords = env.fromElements("data", "DROP", "artisans", "IGNORE", "zhch", "zhch", "artisans", "zhch158", "artisans", "zhch158").keyBy(x -> x);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(2);
		
		control
		    .connect(streamOfWords)
			.process(new ControlFunction())
	        .print();

	    env.execute();
	}
	
	public static class ControlFunction extends KeyedCoProcessFunction<String, String, String, Tuple2<String, String>>  {
		private ValueState<String> controlState;
//		private ValueState<String> wordState;
		private ListState<String> wordState;
			
		@Override
		public void open(Configuration config) {
		    controlState = getRuntimeContext().getState(new ValueStateDescriptor<>("control", String.class));
		    wordState = getRuntimeContext().getListState(new ListStateDescriptor<>("word", String.class));
		}
			
//		@Override
//		public void flatMap1(String control_value, Collector<String> out) throws Exception {
//		    blocked.update(Boolean.TRUE);
//		}
//			
//		@Override
//		public void flatMap2(String data_value, Collector<String> out) throws Exception {
//		    if (blocked.value() == null) {
//			    out.collect(data_value);
//			}
//		}

		@Override
		public void processElement1(String control,
				KeyedCoProcessFunction<String, String, String, Tuple2<String, String>>.Context context,
				Collector<Tuple2<String, String>> out) throws Exception {
			// TODO Auto-generated method stub
			Iterator<String> itWord=wordState.get().iterator();
			if(itWord.hasNext()) {
//				out.collect(new Tuple2("Ignored from processElement1...", word));
				controlState.update(control);
			} else {
				controlState.update(control);
				context.timerService().registerEventTimeTimer(context.timerService().currentWatermark());
			}
		}

		@Override
		public void processElement2(String word,
				KeyedCoProcessFunction<String, String, String, Tuple2<String, String>>.Context context,
				Collector<Tuple2<String, String>> out) throws Exception {
			// TODO Auto-generated method stub
			String control=controlState.value();
			if(control != null) {
//				controlState.clear();
//				out.collect(new Tuple2("Ignored from processElement2...", word));
				wordState.add(word);
			} else {
				wordState.add(word);
				// as soon as the watermark arrives, we can stop waiting for the corresponding ride
				context.timerService().registerEventTimeTimer(context.timerService().currentWatermark());
			}
		}
		
		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
			String control=controlState.value();
//			String word=wordState.value();
			Iterator<String> itWord=wordState.get().iterator();
			if(itWord.hasNext() && control!=null) {
				int i=0;
				while(itWord.hasNext()) {
					String word=itWord.next();
					if(!word.equals(control)) {
						out.collect(new Tuple2("Filtered...["+ ++i +"]", word));
					} else {
	//					out.collect(new Tuple2("Ignored from onTimer...", wordState.value()));
					}
				}
				controlState.clear();
				wordState.clear();
			}
			else if(itWord.hasNext()){
				int i=0;
				while(itWord.hasNext()) {
					String word=itWord.next();
					out.collect(new Tuple2("Filtered from onTimer, control==null...["+ ++i +"]", word));
				}
				wordState.clear();
			}
		}
	}
}
