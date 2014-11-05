/**
 * Graph-Mining Tutorial for Ozone
 *
 * Copyright (C) 2013  Sebastian Schelter <ssc@apache.org>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment2;

public class Config {

  private Config() {}

  public static int numberOfSubtasks() {
    return 2;
  }

  public static String pathToSlashdotZoo() {
    return "/home/ssc/Downloads/out.matrix";
  }

  public static String outputPath() {
    return "/tmp/flink/";
  }

  public static long randomSeed() {
    return 0xdeadbeef;
  }
}
