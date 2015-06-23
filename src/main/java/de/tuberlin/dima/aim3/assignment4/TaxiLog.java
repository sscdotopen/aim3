package de.tuberlin.dima.aim3.assignment4;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by peter on 16.06.15.
 */
public class TaxiLog implements Serializable, Comparable<TaxiLog> {

  private static final long serialVersionUID = 1L;

  public static final int MIN_SPEED = 0;
  public static final int MAX_SPEED = 50;
  public static final int PERIOD = 25 * 1000; // base duration of drive and wait

  private Random rand = new Random();

  private int taxiId;
  private double speed;
  private double distance;

  /**
   * Is set to 25 seconds plus 0 - 20% of itself for randomization of the length of the drive.
   */
  private int taxiMeter;
  private long timestamp;

  /**
   * The threshold determines the ration of waiting / driving of the entire period.
   *
   * For this, the threshold is set to a certain percentage (60 to 80) of the entire
   * period. At the start, the taximeter is set to the period. As soon as the taximeter
   */
  private int threshold;

  public TaxiLog(int taxiId) {
    this(taxiId, 0, 0d, 0l);
  }

  public TaxiLog(int taxiId, double speed, double distance, long timestamp) {
    this.taxiId = taxiId;
    this.speed = speed;
    this.distance = distance;
    this.timestamp = timestamp;
    resetTaxiMeter();
    resetThreshold();
  }

  private void resetTaxiMeter() {
    this.taxiMeter = PERIOD + ((PERIOD / 100) * rand.nextInt(20));
  }

  public void resetThreshold() {
    int percentage = 60 + rand.nextInt(20);
    this.threshold = ((PERIOD / 100) * percentage);
  }

  public void increaseSpeed() {
    if(this.speed < MAX_SPEED) {
      this.speed += 5;
    }
  }

  public void decreaseSpeed() {
    if(this.speed > MIN_SPEED) {
      this.speed -= 5;
    }
  }

  public void exceedTopSpeed() {
    this.speed += 5;
  }

  public void stop() {
    this.speed = 0;
  }

  public void updateDistance() {
    this.distance += this.speed / 3.6d;
  }

  public void updateTaxiMeter() {
    if(taxiMeter <= 0) {
      resetTaxiMeter();
      resetThreshold();
    } else {
      taxiMeter -= 100;
    }
  }

  public boolean isBusy() {
    return this.taxiMeter >= this.threshold;
  }

  public int getIdleTime() {
    return (this.threshold - this.taxiMeter) / 1000;
  }

  public int getTaxiId() {
    return taxiId;
  }

  public void setTaxiId(int taxiId) {
    this.taxiId = taxiId;
  }

  public double getSpeed() {
    return speed;
  }

  public void setSpeed(double speed) {
    this.speed = speed;
  }

  public double getDistance() {
    return distance;
  }

  public void setDistance(double distance) {
    this.distance = distance;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return taxiId + ", " + speed + ", " + distance  + ", " + timestamp;
  }

  @Override
  public int compareTo(TaxiLog other) {
    return this.taxiId - other.taxiId;
  }

}
