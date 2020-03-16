package com.ververica.flinktraining.zhch;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com). The task of the exercise is to filter a data
 * stream of taxi ride records to keep only rides that start and end within New
 * York City. The resulting stream should be printed.
 *
 * Parameters: -input path-to-input-file
 *
 */
public class RideCleansingExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// DataStream<TaxiRide> filteredRides = rides
		// // filter out rides that do not start or stop in NYC
		// .filter(new NYCFilter());
		//
		// // print the filtered stream
		// printOrTest(filteredRides);

		// DataStream<EnrichedRide> enrichedNYCRides = rides
		// .filter(new NYCFilter())
		// .map(new Enrichment());
		//
		// enrichedNYCRides.print();

		DataStream<EnrichedRide> enrichedNYCRides = rides
				.flatMap(new NYCEnrichment())
				.keyBy(EnrichedRide->EnrichedRide.startCell)	//lambda表达式
//				.keyBy(EnrichedRide::getStartCell)
				;

//		enrichedNYCRides.print(); // run the cleansing pipeline
		
		DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides
				.flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
					@Override
					public void flatMap(EnrichedRide ride, Collector<Tuple2<Integer, Minutes>> out) throws Exception {
						if (!ride.isStart) {
							Interval rideInterval = new Interval(ride.startTime, ride.endTime);
							Minutes duration = rideInterval.toDuration().toStandardMinutes();
							out.collect(new Tuple2<>(ride.startCell, duration));
						}
					}
				});
		minutesByStartCell
	    .keyBy(0) // startCell
	    //.maxBy(1) // duration
	    .reduce(new ReduceFunction<Tuple2<Integer,Minutes>>() {
			
			@Override
			public Tuple2<Integer, Minutes> reduce(Tuple2<Integer, Minutes> value1, Tuple2<Integer, Minutes> value2)
					throws Exception {
				// TODO Auto-generated method stub
				if(value1.f1.getMinutes()>value2.f1.getMinutes())
					return value1;
				else
					return value2;
			}
		})
	    .print()
	    ;
		
		env.execute("Taxi Ride Cleansing");
	}

	private static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			// throw new MissingSolutionException();
			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
					&& GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

	public static class EnrichedRide extends TaxiRide {
		public int startCell;
		public int endCell;

		public int getStartCell() {
			return startCell;
		}

		public void setStartCell(int startCell) {
			this.startCell = startCell;
		}

		public int getEndCell() {
			return endCell;
		}

		public void setEndCell(int endCell) {
			this.endCell = endCell;
		}

		public EnrichedRide() {
		}

		public EnrichedRide(TaxiRide ride) {
			this.rideId = ride.rideId;
			this.isStart = ride.isStart;
			this.startTime = ride.startTime;
			this.endTime = ride.endTime;
			this.startLon = ride.startLon;
			this.startLat = ride.startLat;
			this.endLon = ride.endLon;
			this.endLat = ride.endLat;
			this.passengerCnt = ride.passengerCnt;
			this.taxiId = ride.taxiId;
			this.driverId = ride.driverId;
			this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
			this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
		}

		public String toString() {
			return super.toString() + "," + Integer.toString(this.startCell) + "," + Integer.toString(this.endCell);
		}
	}

	public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
		@Override
		public EnrichedRide map(TaxiRide taxiRide) throws Exception {
			return new EnrichedRide(taxiRide);
		}
	}

	public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
		@Override
		public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
			FilterFunction<TaxiRide> valid = new NYCFilter();
			if (valid.filter(taxiRide)) {
				out.collect(new EnrichedRide(taxiRide));
			}
		}
	}

}
