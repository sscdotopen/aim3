package de.tuberlin.dima.aim3.assignment2.chainletter;

import de.tuberlin.dima.aim3.assignment2.Config;
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

public class ChainLetter {
	// The comments show the answers for each question by Q[number of
	// questions]: answers
	// ex. Q3:This algorithm is guaranteed to converge
	// because the max iteration is 3 according to
	// selectedInitiators.iterateDelta(initialForwards, 3, 0);
	// Besides, it will terminate after all vertexes have received message
	// and the number of vertexes isn't infinite

	public static final double INITIATOR_RATIO = 0.00125;
	public static final double FORWARDING_PROBABILITY = 0.5;

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

		// Q3:Map edges to (source,target,isFriend)
		// Q5:Ensure the massages are sent only between friends
		DataSet<Tuple2<Long, Long>> friendEdges = input
				.flatMap(new FriendEdgeReader());

		// Q3:Get the source vertexes
		DataSet<Tuple1<Long>> edgesToVertices = friendEdges.project(0).types(
				Long.class);

		// Q3:Get the cardinality of source vertexes
		DataSet<Tuple1<Long>> uniqueVertexIds = edgesToVertices.distinct();

		// Q3:Choose random vertexes as initial vertex
		DataSet<Tuple2<Long, Boolean>> selectedInitiators = uniqueVertexIds
				.map(new SelectInitiators());

		// Q3:The edges are chosen to send at first round by joining the random
		// chosen vertexes with edges and select partial edges
		// Q4:Can configure the join strategy by using
		// selectedInitiators.join(edges, strategy)
		// |Edges| is bigger than |selectedInitiators|.
		// The density of graph(edges/|nodes|) can determine it's much bigger or
		// a bit bigger
		// The following strategy can be considered:
		// BROADCAST_HASH_FIRST
		// Hint that the first join input is much smaller than the second.
		// REPARTITION_HASH_FIRST
		// Hint that the first join input is a bit smaller than the second.
		// Ref:http://flink.incubator.apache.org/docs/0.7-incubating/api/java/org/apache/flink/api/java/operators/JoinOperator.JoinHint.html
		DataSet<Tuple2<Long, Long>> initialForwards = selectedInitiators
				.join(friendEdges).where(0).equalTo(0)
				.flatMap(new InitialForwards());

		// Iterate 3 times and the key position is 0. The first workset is selectedInitiators
		DeltaIteration<Tuple2<Long, Boolean>, Tuple2<Long, Long>> deltaIteration = selectedInitiators
				.iterateDelta(initialForwards, 3, 0);

		// Q3:Get the target vertexes which receive message and they should then be put in the solution set
		DataSet<Tuple1<Long>> deliverMessage = deltaIteration.getWorkset()
				.project(1).types(Long.class).distinct();

		// Q3:Not every vertex in the solution set will send message in each iteration.
		// This allows to focus on the vertexes should send message
		// and leave the vertexes didn't send message untouched
		DataSet<Tuple2<Long, Boolean>> nextRecipientStates = deltaIteration
				.getSolutionSet().join(deliverMessage).where(0).equalTo(0)
				.flatMap(new ReceiveMessage());

		// Q3:Send message from the vertexes has been sent
		DataSet<Tuple2<Long, Long>> nextForwards = nextRecipientStates
				.join(friendEdges).where(0).equalTo(0)
				.flatMap(new ForwardToFriend());

		DataSet<Tuple2<Long, Boolean>> result = deltaIteration.closeWith(
				nextRecipientStates, nextForwards);

		result.print();

		env.execute();
	}

	public static class FriendEdgeReader implements
			FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);
				boolean isFriend = "+1".equals(tokens[2]);
				// Q5: ensure the messages are only forwarded between friends
				if (isFriend)
					collector.collect(new Tuple2<Long, Long>(source, target));
			}
		}
	}

	// Choose random vertexes as initial vertexes
	public static class SelectInitiators implements
			MapFunction<Tuple1<Long>, Tuple2<Long, Boolean>> {

		private final Random random = new Random(Config.randomSeed());

		@Override
		public Tuple2<Long, Boolean> map(Tuple1<Long> vertex) throws Exception {

			boolean isSeedVertex = random.nextDouble() < ChainLetter.INITIATOR_RATIO;
			return new Tuple2<Long, Boolean>(vertex.f0, isSeedVertex);
		}
	}

	// The edges of vertexes will send message at first round
	public static class InitialForwards
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		private final Random random = new Random(Config.randomSeed());

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>> vertexWithEdge,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			Tuple2<Long, Boolean> vertex = vertexWithEdge.f0;
			Tuple2<Long, Long> edge = vertexWithEdge.f1;
			boolean isSeedVertex = vertex.f1;

			if (isSeedVertex
					&& random.nextDouble() < ChainLetter.FORWARDING_PROBABILITY) {
				// the chosen edges were sent message
				collector.collect(edge);
			}

		}
	}

	// Update solution set 
	public static class ReceiveMessage
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple1<Long>>, Tuple2<Long, Boolean>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Boolean>, Tuple1<Long>> recipients,
				Collector<Tuple2<Long, Boolean>> collector) throws Exception {
			Tuple2<Long, Boolean> recipient = recipients.f0;
			// The vertexes should be sent message
			boolean alreadyReceived = recipient.f1;
			if (!alreadyReceived) {
				collector
						.collect(new Tuple2<Long, Boolean>(recipient.f0, true));
			}
		}
	}

	// Workset
	public static class ForwardToFriend
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		private final Random random = new Random(Config.randomSeed());

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>> recipientsAndEdge,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			if (random.nextDouble() < ChainLetter.FORWARDING_PROBABILITY) {
				Tuple2<Long, Long> edge = recipientsAndEdge.f1;
				collector.collect(edge);
			}
		}
	}
}
