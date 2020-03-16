/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.zhch;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;

import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

//		DataStream<Tuple3<String, Long, Float>> driverTips=fares
//				.keyBy((TaxiFare x)->x.driverId)
//				.window(TumblingEventTimeWindows.of(Time.hours(1)))
//				.process(new addTips())
//				;
		DataStream<Tuple3<String, Long, Float>> driverTips=fares
				.map(new MapFunction<TaxiFare, Tuple2<Long, Float>>() {

					@Override
					public Tuple2<Long, Float> map(TaxiFare value) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<>(value.driverId, value.tip);
					}
				})
				.setParallelism(1)
				.keyBy((Tuple2<Long, Float> x)->x.f0)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.reduce(new addTips1(), new WrapWithWindowInfo())
				;
		
		
		DataStream<Tuple3<String, Long, Float>> hourlyMax=driverTips
				.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
				.max(2)
				;
		
		
		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	private static class addTips1 implements ReduceFunction<Tuple2<Long, Float>> {
//		Tuple2<Long, Float> sumTips = new Tuple2<Long, Float>();
//		ThreadLocal<Tuple2<Long, Float>> sumTipsThreadLocal=new ThreadLocal<Tuple2<Long, Float>>();
//		Tuple2<Long, Float> sumTips=sumTipsThreadLocal.get();

		@Override
		public Tuple2<Long, Float> reduce(Tuple2<Long, Float> value1, Tuple2<Long, Float> value2) throws Exception {
			// TODO Auto-generated method stub
			
//			return new Tuple2<>(value1.f0, value1.f1+value2.f1);
			
			Tuple2<Long, Float> sumTips = new Tuple2<Long, Float>();
			sumTips.f0=value1.f0;
			sumTips.f1=value1.f1+value2.f1;
			return sumTips;
		}
	}
	
	private static class WrapWithWindowInfo extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<String, Long, Float>, Long, TimeWindow> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		@Override
		public void process(Long key,
				ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<String, Long, Float>, Long, TimeWindow>.Context context,
				Iterable<Tuple2<Long, Float>> events, Collector<Tuple3<String, Long, Float>> out) throws Exception {
			// TODO Auto-generated method stub
			out.collect(new Tuple3<>(sdf.format(context.window().getEnd()), key, events.iterator().next().f1));
		}
	}
	
	private static class addTips extends ProcessWindowFunction<TaxiFare, Tuple3<String, Long, Float>, Long, TimeWindow> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		@Override
		public void process(Long key,
				ProcessWindowFunction<TaxiFare, Tuple3<String, Long, Float>, Long, TimeWindow>.Context context,
				Iterable<TaxiFare> events, Collector<Tuple3<String, Long, Float>> out) throws Exception {
			// TODO Auto-generated method stub
			float tips=0;
			for(TaxiFare event: events) {
				tips += event.tip;
			}
			out.collect(new Tuple3<>(sdf.format(context.window().getEnd()), key, tips));
		}
	}
}

