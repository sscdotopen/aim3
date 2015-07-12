/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.regex.Pattern;

public class ChainLetterForFriends {

  public static final double INITIATOR_RATIO = 0.00125;
  public static final double FORWARDING_PROBABILITY = 0.5;

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

    /*
     * Get all edges (connections) between vertices from input file.
     * The flatMap operator produces a data set for the input sting (line in file).
     * The type of connection (friend or foe) is not required here.
     *
     * output set: Tuple 2 (src id, dest id)
     */
    DataSet<Tuple2<Long, Long>> edges = input.flatMap(new EdgeReader());

    /*
     * EdgesToVertices
     *
     * Map the 2-tuple to a 1-tuple by ignoring the second value of the tuple.
     * The result are all ids of source vertices.
     *
     * input set: Tuple 2 (src id, dest id)
     *
     * output set: Tuple 1 (src id) <= all source vertices, not unique
     */
    DataSet<Tuple1<Long>> edgesToVertices = edges.project(0).types(Long.class);

    /*
     * UniqueVertexIDs
     *
     * Reduce source ids (distinct) to gain unique set of vertices.
     *
     * input set: Tuple 1 (src id)
     *
     * output set: Tuple 1 (vertex id) <= unique vertex
     */
    DataSet<Tuple1<Long>> uniqueVertexIds = edgesToVertices.distinct();

    /*
     * SelectInitiators
     *
     * Map boolean (seed/notSeed) to each vertex, by using random value and initiator ratio.
     *
     * input set: Tuple 1 (vertex id)
     *
     * output set: Tuple 2 (vertex id, seed/notSeed)
     */
    DataSet<Tuple2<Long, Boolean>> selectedInitiators = uniqueVertexIds.map(new SelectInitiators());

    /*
     * InitialForwards
     *
     * Join the edges to the labeled vertices, to get the destinations of seeds.
     * Find the initial forwards: if seed && probability of 50%.
     * The output are the edges from initial seeds to their destination.
     *
     * input: Tuple 2 (Tuple 1 (vertex id), Tuple 2 (src id, dest id))
     *
     * output: Tuple 2 (src id, dest id)
     */
    DataSet<Tuple2<Long, Long>> initialForwards =
        selectedInitiators.join(edges).where(0).equalTo(0)
                          .flatMap(new InitialForwards());

    /*
     * DeltaIteration
     *
     * iterations terminate when the workingSet (set which is fed back) becomes empty.
     * The iteration diverges either because for the update of the working set of each iteration,
     * then, when the next forward is determined, the tuples from the solutionSet that
     * are marked with a 'true' are not merged in the next working set anymore.
     * It becomes smaller as in every iteration the receiver tuple is marked with 'true'.
     * Or because the number of maximum iterations is set to 3.
     *
     * WorkingSet (initialSet):
     *                          ForwardToFriend:  Tuple 2 (src id, dest id)
     *                          (InitialForwards: Tuple 2 (src id, dest id))
     * the currently served set of data (edges) each iteration.
     *
     * SolutionSet (initialState):
     *                          ReceiveMessage:      Tuple 2 (vertex id, received/notReceived)
     *                          (SelectedInitiators: Tuple 2 (vertex id, seed/notSeed))
     * current state at the beginning of each iteration.
     *
     * max iterations: 3 => iteration diverges definitely.
     *
     * output set: Tuple 2 (Tuple 2 (vertex id, seed/notSeed), Tuple 2 (src id, dest id))
     */
    DeltaIteration<Tuple2<Long, Boolean>, Tuple2<Long, Long>> deltaIteration =
        selectedInitiators.iterateDelta(initialForwards, 3, 0);

    /*
     * DeliverMessage
     *
     * The new unique, potential destinations for message per iteration (first tuple field of workingSet) are determined.
     * This is done by getting the distinct set of destination vertex id from the current working set.
     * The current working set contains the edges which were served in past iteration.
     *
     * input set: Tuple 2 (src id, dest id)
     *
     * output set: Tuple 1 (vertex id)
     */
    DataSet<Tuple1<Long>> deliverMessage =
        deltaIteration.getWorkset().<Tuple1<Long>>project(1).distinct();

    /*
     * ReceiveMessage
     *
     * Join of solution set and unique potential destination set.
     * The flatMap operator produces the output tuple under consideration of received/notReceived status of destination vertex.
     * This building the solution set of the iteration.
     *
     * input set: Tuple 2 (Tuple 2 (vertex id, received/notReceived), Tuple 1 (dest id))
     *
     * output set: Tuple 2 (destination id, received/notReceived<false>)
     */
    DataSet<Tuple2<Long, Boolean>> nextRecipientStates =
        deltaIteration.getSolutionSet()
                          .join(deliverMessage).where(0).equalTo(0)
                          .flatMap(new ReceiveMessage());

    /*
     * ForwardToFriend
     *
     * Find the next forwards (working set for next iteration) by probability (50%).
     * By joining the edges to the receivers, the new working set is build.
     * The output edges are served in the next iteration.
     *
     * input: Tuple 2 (Tuple 2 (dest id, received), Tuple 2 (src id, dest id))
     *
     * output: Tuple 2 (src id, dest id)
     */
    DataSet<Tuple2<Long, Long>> nextForwards =
        nextRecipientStates.join(edges).where(0).equalTo(0)
                           .flatMap(new ForwardToFriend());

    /*
     * Result
     *
     * Close the iteration and get the result. The result represents the edges which were not served.
     * Here: the connections where the chain letter was not sent to.
     * If the set is empty this means every node was served, if not, the letter has died because of the 50%-rule.
     *
     * input set: Tuple 2 (Tuple 2 (vertex id, received), Tuple 2 (src id, dest id))
     *
     * output set: resulting working set.
     */
    DataSet<Tuple2<Long, Boolean>> result =
        deltaIteration.closeWith(nextRecipientStates, nextForwards);

    result.print();

    env.execute();
  }

  public static class EdgeReader implements FlatMapFunction<String, Tuple2<Long, Long>> {

    private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");
    private static final String FRIEND_LABEL = "+1";

    @Override
    public void flatMap(String s, Collector<Tuple2<Long, Long>> collector) throws Exception {
      if (!s.startsWith("%")) {
        String[] tokens = SEPARATOR.split(s);

        long source = Long.parseLong(tokens[0]);
        long target = Long.parseLong(tokens[1]);
        boolean isFriend = FRIEND_LABEL.equals(tokens[2]);

        if(isFriend) {
          /* only add the edges between friends */
          collector.collect(new Tuple2<Long, Long>(source, target));
        }
      }
    }

  }

  public static class SelectInitiators implements MapFunction<Tuple1<Long>,Tuple2<Long,Boolean>> {

    private final Random random = new Random(Config.randomSeed());

    @Override
    public Tuple2<Long, Boolean> map(Tuple1<Long> vertex) throws Exception {

      boolean isSeedVertex = random.nextDouble() < ChainLetterForFriends.INITIATOR_RATIO;
      return new Tuple2<Long, Boolean>(vertex.f0, isSeedVertex);
    }
  }

  public static class InitialForwards
      implements FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

    private final Random random = new Random(Config.randomSeed());

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>> vertexWithEdge, Collector<Tuple2<Long, Long>> collector)
        throws Exception {

      Tuple2<Long, Boolean> vertex = vertexWithEdge.f0;
      Tuple2<Long, Long> edge = vertexWithEdge.f1;
      boolean isSeedVertex = vertex.f1;

      if (isSeedVertex && random.nextDouble() < ChainLetterForFriends.FORWARDING_PROBABILITY) {
        collector.collect(edge);
      }

    }
  }

  public static class ReceiveMessage
      implements FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple1<Long>>, Tuple2<Long, Boolean>> {

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Boolean>, Tuple1<Long>> recipients, Collector<Tuple2<Long, Boolean>> collector)
        throws Exception {
      Tuple2<Long, Boolean> recipient = recipients.f0;

      boolean alreadyReceived = recipient.f1;
      if (!alreadyReceived) {
        collector.collect(new Tuple2<Long, Boolean>(recipient.f0, true));
      }
    }
  }

  public static class ForwardToFriend
      implements FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

    private final Random random = new Random(Config.randomSeed());

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>> recipientsAndEdge,
                        Collector<Tuple2<Long, Long>> collector) throws Exception {

      if (random.nextDouble() < ChainLetterForFriends.FORWARDING_PROBABILITY) {
        Tuple2<Long, Long> edge = recipientsAndEdge.f1;
        collector.collect(edge);
      }
    }
  }
}
