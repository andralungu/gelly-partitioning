package library;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;
import splitUtils.SplitVertex;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class NodeSplittingCommunityDetection implements GraphAlgorithm<String, Tuple2<String, Long>, Double> {

	private Integer maxIterations;
	private Double delta;
	private Integer level;

	public NodeSplittingCommunityDetection(Integer maxIterations, Double delta, Integer level) {
		this.maxIterations = maxIterations;
		this.delta = delta;
		this.level = level;
	}

	@Override
	public Graph<String, Tuple2<String, Long>, Double> run(Graph<String, Tuple2<String, Long>, Double> graph) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<String, Tuple2<String, Long>, Double> undirectedGraph = graph.getUndirected();
		// the vertex value will contain tag, label, score, receivedLabelWithScore,
		Graph<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>, Double> graphWithScoredVertices
				= undirectedGraph.mapVertices(new AddScoresAndTreeMapsToVertexValues());

		DataSet<Vertex<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> aggregatedVertices =
				graphWithScoredVertices.getVertices().map(new WipeTag());
		DataSet<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> updatedSplitVertices
				= graphWithScoredVertices.getVertices();


		for(int i = 0; i < maxIterations; i++) {
			// run an iteration
			Graph<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>, Double> graphAfterOneIteration =
			Graph.fromDataSet(updatedSplitVertices, graphWithScoredVertices.getEdges(), env)
			.runVertexCentricIteration(new VertexLabelUpdater(), new LabelMessenger(), 1);
			// aggregate
			aggregatedVertices =
					SplitVertex.treeAggregate(graphAfterOneIteration.getVertices(), level, new Aggregate(delta, i))
					.map(new WipeTag());

			// propagate the aggregated values to the split vertices
			updatedSplitVertices =
					SplitVertex.propagateValuesToSplitVertices(graphAfterOneIteration.getVertices(), aggregatedVertices);
			updatedSplitVertices.print();
		}

		return Graph.fromDataSet(aggregatedVertices.map(new MapFunction<Vertex<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>, Vertex<String, Tuple2<String, Long>>>() {
					@Override
					public Vertex<String, Tuple2<String, Long>> map(Vertex<String, Tuple4<Long, Double, Map<Long, Double>,
							Map<Long, Double>>> vertex) throws Exception {
						return new Vertex<String, Tuple2<String, Long>>(vertex.getId(), new Tuple2<String, Long>(vertex.getId(),
								vertex.getValue().f0));
					}
				}),
				graphWithScoredVertices.getEdges(), env);

	}

	@SuppressWarnings("serial")
	public static final class WipeTag implements MapFunction<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>,
			Vertex<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> {

		@Override
		public Vertex<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>> map(
				Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> vertex) throws Exception {
			return new Vertex<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>(vertex.getId(),
					vertex.getValue().f1);
		}
	}

	@SuppressWarnings("serial")
	public static class AddScoresAndTreeMapsToVertexValues implements
			MapFunction<Vertex<String, Tuple2<String, Long>>, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> {

		@Override
		public Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>> map(Vertex<String, Tuple2<String, Long>> vertex) throws Exception {
			return new Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>(vertex.getValue().f0,
					new Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>(vertex.getValue().f1,
					1.0, new TreeMap<Long, Double>(), new TreeMap<Long, Double>()));
		}
	}

	@SuppressWarnings("serial")
	public static final class VertexLabelUpdater extends VertexUpdateFunction<String,
			Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>,
			Tuple2<Long, Double>> {

		@Override
		public void updateVertex(Vertex<String, Tuple2<String,
				Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> vertex,
								 MessageIterator<Tuple2<Long, Double>> messages) throws Exception {

			// we would like these two maps to be ordered
			Map<Long, Double> receivedLabelsWithScores = new TreeMap<Long, Double>();
			Map<Long, Double> labelsWithHighestScore = new TreeMap<Long, Double>();

			for(Tuple2<Long, Double> message: messages) {
				// split the message into received label and score
				Long receivedLabel = message.f0;
				Double receivedScore = message.f1;

				// if the label was received before
				if (receivedLabelsWithScores.containsKey(receivedLabel)) {
					Double newScore = receivedScore + receivedLabelsWithScores.get(receivedLabel);
					receivedLabelsWithScores.put(receivedLabel, newScore);
				} else {
					// first time we see the label
					receivedLabelsWithScores.put(receivedLabel, receivedScore);
				}

				// store the labels with the highest scores
				if (labelsWithHighestScore.containsKey(receivedLabel)) {
					Double currentScore = labelsWithHighestScore.get(receivedLabel);
					if (currentScore < receivedScore) {
						// record the highest score
						labelsWithHighestScore.put(receivedLabel, receivedScore);
					}
				} else {
					// first time we see this label
					labelsWithHighestScore.put(receivedLabel, receivedScore);
				}
			}

			// the label and the score remain the same; they will be computed in the merging phase
			setNewVertexValue(new Tuple2<String, Tuple4<Long, Double, Map<Long,Double>, Map<Long,Double>>>(
					vertex.getValue().f0, new Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>(
					vertex.getValue().f1.f0, vertex.getValue().f1.f1, receivedLabelsWithScores, labelsWithHighestScore)));
		}
	}

	@SuppressWarnings("serial")
	public static final class LabelMessenger extends MessagingFunction<String,
			Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>,
			Tuple2<Long, Double>, Double> {
		@Override
		public void sendMessages(Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> vertex) throws Exception {
			for(Edge<String, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), new Tuple2<Long, Double>(vertex.getValue().f1.f0,
						vertex.getValue().f1.f1 * edge.getValue()));
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class Aggregate implements
			GroupReduceFunction<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>,
					Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> {

		private Double delta;
		private int superstepNumber;

		public Aggregate (Double delta, int superstepNumber) {
			this.delta = delta;
			this.superstepNumber = superstepNumber;
		}

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> vertices,
						   Collector<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> vertexIterator =
					vertices.iterator();
			Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> next = null;
			Map<Long, Double> receivedLabelsWithScores = new TreeMap<Long, Double>();
			Map<Long, Double> labelsWithHighestScore = new TreeMap<Long, Double>();

			Map<Long, Double> auxReceivedLabelsWithScores = new TreeMap<Long, Double>();
			Map<Long, Double> auxLabelsWithHighestScore = new TreeMap<Long, Double>();

			String id = null;

			while (vertexIterator.hasNext()) {
				next = vertexIterator.next();
				// the values will also have subvertices; the aux tree maps are used to form the final
				// tree maps
				auxReceivedLabelsWithScores = next.getValue().f1.f2;
				auxLabelsWithHighestScore = next.getValue().f1.f3;
				id = next.getValue().f0;

				for(Long key: auxReceivedLabelsWithScores.keySet()) {
					// if the label was received before
					if (receivedLabelsWithScores.containsKey(key)) {
						Double newScore = auxReceivedLabelsWithScores.get(key) + receivedLabelsWithScores.get(key);
						receivedLabelsWithScores.put(key, newScore);
					} else {
						// first time we see the label
						receivedLabelsWithScores.put(key, auxReceivedLabelsWithScores.get(key));
					}

					// store the labels with the highest scores
					if (auxLabelsWithHighestScore.containsKey(key)) {
						Double currentScore = auxLabelsWithHighestScore.get(key);
						if (currentScore < key) {
							// record the highest score
							labelsWithHighestScore.put(key, auxLabelsWithHighestScore.get(key));
						}
					} else {
						// first time we see this label
						labelsWithHighestScore.put(key, auxLabelsWithHighestScore.get(key));
					}
				}
			}

			if(receivedLabelsWithScores.size() > 0) {
				// find the label with the highest score from the ones received
				Double maxScore = -Double.MAX_VALUE;
				Long maxScoreLabel = next.getValue().f1.f0;
				for (Long curLabel : receivedLabelsWithScores.keySet()) {
					if (receivedLabelsWithScores.get(curLabel) > maxScore) {
						maxScore = receivedLabelsWithScores.get(curLabel);
						maxScoreLabel = curLabel;
					}
				}
				// find the highest score of maxScoreLabel
				Double highestScore = labelsWithHighestScore.get(maxScoreLabel);
				// re-score the new label
				if (maxScoreLabel != next.getValue().f1.f0) {
					highestScore -= delta / superstepNumber;
				}

				// else delta = 0
				// update own label
				collector.collect(new Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>(id,
						new Tuple2<String, Tuple4<Long, Double, Map<Long,Double>, Map<Long,Double>>>(id,
								new Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>(
										maxScoreLabel, highestScore, receivedLabelsWithScores, labelsWithHighestScore))));
			}
		}
	}
}
