package de.tuberlin.dima.aim3.assignment4;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.helper.*;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

import java.util.Random;

/**
 * An example of grouped stream windowing where different eviction and trigger
 * policies can be used. A source fetches events from cars every 1 sec
 * containing their id, their current speedLimit (kmh), overall elapsed distance (m)
 * and a timestamp. The streaming example triggers the top speedLimit of each car
 * every x meters elapsed for the last y seconds.
 */
public class TaxiMonitor {

  private static final int NUM_TAXI_EVENTS = 100;

  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  public static void main(String[] args) throws Exception {

    if (!parseParameters(args)) {
      return;
    }

    final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<TaxiLog> taxiData;
    if (fileInput) {
      taxiData = env.readTextFile(inputPath).<TaxiLog>map(new ParseTaxiData());
    } else {
      taxiData = env.addSource(TaxiSource.create(numOfTaxis));
    }

    /*
     * find the top speed of each driver within the last window (evictionSec)
     */
    DataStream<TaxiLog> topSpeeds =
        taxiData.groupBy(new TaxiIdKeySelector()) /* create buckets of drivers */
            .window(Time.of(evictionSec, new TaxiTimestamp()))
            .every(Delta.of(triggerMeters, new TaxiDistanceDeltaFunction(), new TaxiLog(0)))
            .local()
            .maxBy("speed")
            .flatten();

    /*
     * find drivers which exceed a certain limit (speedLimit)
     */
    DataStream<TaxiLog> exceedMaxSpeeds =
       taxiData.window(SpeedViolation.of(speedLimit)) /* trigger to return a windows
                                                         when the speed limit is violated*/
           .every(Count.of(1))
           .local()
           .maxBy(1) /* get the one with the biggest speed */
           .flatten();

    /*
     * find the drivers which idle for longer then x seconds (idleThreshold)
     */
    DataStream<TaxiLog> idles =
        taxiData.window(Time.of(1000, new TaxiTimestamp())) /* trigger a window every second */
            .local()
            .maxBy(3) /* get the log with the biggest timestamp (last log) */
            .flatten()
            .filter(taxiLog -> taxiLog.getIdleTime() > idleThreshold); /* filter those with idle */

    if (fileOutput) {
      topSpeeds.writeAsText(outputPath);
      exceedMaxSpeeds.writeAsText(outputPath);
      idles.writeAsText(outputPath);
    } else {
      topSpeeds.print();
      exceedMaxSpeeds.print();
      idles.print();
    }

    env.execute("TaxiMonitor");
  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  private static class TaxiSource implements SourceFunction<TaxiLog> {

    private static final long serialVersionUID = 1L;

    public static final int LIKELIHOOD_EXCEED_MAX_SPEED = 75;

    private TaxiLog[] taxiLogs;

    private Random rand = new Random();

    private volatile boolean isRunning = true;
    private int counter;

    private TaxiSource(int numOfTaxis) {
      taxiLogs = new TaxiLog[numOfTaxis];
      for(int i = 0; i < taxiLogs.length; i++) {
        taxiLogs[i] = new TaxiLog(i+1);
      }
    }

    public static TaxiSource create(int numOfTaxis) {
      return new TaxiSource(numOfTaxis);
    }

    @Override
    public void run(SourceContext<TaxiLog> ctx) throws Exception {

      while (isRunning && counter < NUM_TAXI_EVENTS) {
        Thread.sleep(100);
        for (int taxiId = 0; taxiId < taxiLogs.length; taxiId++) {
          if (taxiLogs[taxiId].isBusy()) {
            // taxi is busy, so do the regular driving
            if ((taxiLogs[taxiId].getSpeed() == TaxiLog.MAX_SPEED)
                && (rand.nextInt(100) > LIKELIHOOD_EXCEED_MAX_SPEED)) {
              // if we are on max speedLimit, occasionally (probability: 25%) it is exceeded
              taxiLogs[taxiId].exceedTopSpeed();
            } else {
              // otherwise its a 50/50 chance to accelerate/decelerate
              if (rand.nextBoolean()) {
                taxiLogs[taxiId].increaseSpeed();
              } else {
                taxiLogs[taxiId].decreaseSpeed();
              }
            }
            // while driving update the distance according to the current speed
            taxiLogs[taxiId].updateDistance();
          } else {
            // the driving time is over (underflow of threshold); stop the taxi
            taxiLogs[taxiId].stop();
          }

          // decrease the taximeter every second
          taxiLogs[taxiId].updateTaxiMeter();
          // update the timestamp
          taxiLogs[taxiId].setTimestamp(System.currentTimeMillis());

          // provide the taxi log to the stream
          ctx.collect(taxiLogs[taxiId]);
          counter++;
        }
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

  private static class ParseTaxiData extends RichMapFunction<String, TaxiLog> {
    private static final long serialVersionUID = 1L;
    @Override
    public TaxiLog map(String record) {

      String rawData = record.substring(1, record.length() - 1);
      String[] data = rawData.split(",");

      return new TaxiLog(Integer.valueOf(data[0]),
          Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
    }
  }

  private static class TaxiIdKeySelector implements KeySelector<TaxiLog, Integer> {
    private static final long serialVersionUID = 1L;
    @Override
    public Integer getKey(TaxiLog value) throws Exception {
      return value.getTaxiId();
    }
  }

  private static class TaxiDistanceDeltaFunction implements DeltaFunction<TaxiLog> {
    private static final long serialVersionUID = 1L;
    @Override
    public double getDelta(TaxiLog oldDataPoint, TaxiLog newDataPoint) {
      return newDataPoint.getDistance() - oldDataPoint.getDistance();
    }
  }

  private static class TaxiTimestamp implements Timestamp<TaxiLog> {
    private static final long serialVersionUID = 1L;
    @Override
    public long getTimestamp(TaxiLog value) {
      return value.getTimestamp();
    }
  }

  private static class SpeedViolation extends WindowingHelper<TaxiLog> {

    private int speedLimit;

    public SpeedViolation(int speedLimit) {
      this.speedLimit = speedLimit;
    }

    @Override
    public EvictionPolicy<TaxiLog> toEvict() {
      return new CountEvictionPolicy<>(0);
    }

    @Override
    public TriggerPolicy<TaxiLog> toTrigger() {
      return new SpeedViolationTrigger(this.speedLimit);
    }

    public static SpeedViolation of(int speedLimit) {
      return new SpeedViolation(speedLimit);
    }

    private static class SpeedViolationTrigger implements CloneableTriggerPolicy<TaxiLog> {

      private int speedLimit;

      public SpeedViolationTrigger(int speedLimit) {
        this.speedLimit = speedLimit;
      }

      @Override
      public boolean notifyTrigger(TaxiLog value) {
        boolean trigger = value.getSpeed() > speedLimit;
        if(trigger) System.out.println(value.getTimestamp());
        return trigger;
      }

      @Override
      public CloneableTriggerPolicy<TaxiLog> clone() {
        return new SpeedViolationTrigger(this.speedLimit);
      }
    }

  }

  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************

  private static boolean fileInput = false;
  private static boolean fileOutput = false;
  private static int numOfTaxis = 2;
  private static int evictionSec = 10;
  private static int speedLimit = 50;
  private static int idleThreshold = 5;
  private static double triggerMeters = 50;
  private static String inputPath;
  private static String outputPath;

  private static boolean parseParameters(String[] args) {

    if (args.length > 0) {
      if (args.length == 2) {
        fileInput = true;
        fileOutput = true;
        inputPath = args[0];
        outputPath = args[1];
      } else {
        System.err.println("Usage: TaxiMonitor <input path> <output path>");
        return false;
      }
    }
    return true;
  }

}
