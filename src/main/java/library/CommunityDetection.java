package library;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

import java.util.Map;
import java.util.TreeMap;

/**
 * Community Detection Algorithm.
 *
 * Initially, each vertex is assigned a tuple formed of its own id along with a score equal to 1.0, as value.
 * The vertices propagate their labels and max scores in iterations, each time adopting the label with the
 * highest score from the list of received messages. The chosen label is afterwards re-scored using the fraction
 * delta/the superstep number. Delta is passed as a parameter and has 0.5 as a default value.
 *
 * The algorithm converges when vertices no longer update their value or when the maximum number of iterations
 * is reached.
 *
 * @see <a href="http://arxiv.org/pdf/0808.2633.pdf">article explaining the algorithm in detail</a>
 */
public class CommunityDetection implements GraphAlgorithm<String, Long, Double> {

	private Integer maxIterations;
	private Double delta;

	public CommunityDetection(Integer maxIterations, Double delta) {
		this.maxIterations = maxIterations;
		this.delta = delta;
	}

	@Override
	public Graph<String, Long, Double> run(Graph<String, Long, Double> graph) throws Exception {

		Graph<String, Long, Double> undirectedGraph = graph.getUndirected();
		Graph<String, Tuple2<Long, Double>, Double> graphWithScoredVertices = undirectedGraph
				.mapVertices(new AddScoreToVertexValuesMapper());
		return graphWithScoredVertices.runVertexCentricIteration(new VertexLabelUpdater(delta),
				new LabelMessenger(), maxIterations)
				.mapVertices(new RemoveScoreFromVertexValuesMapper());
	}

	@SuppressWarnings("serial")
	public static final class VertexLabelUpdater extends VertexUpdateFunction<String, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		private Double delta;

		public VertexLabelUpdater(Double delta) {
			this.delta = delta;
		}

		@Override
		public void updateVertex(Vertex<String, Tuple2<Long, Double>> vertex,
								 MessageIterator<Tuple2<Long, Double>> inMessages) throws Exception {

			// we would like these two maps to be ordered
			Map<Long, Double> receivedLabelsWithScores = new TreeMap<Long, Double>();
			Map<Long, Double> labelsWithHighestScore = new TreeMap<Long, Double>();

			for (Tuple2<Long, Double> message : inMessages) {
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

			if(receivedLabelsWithScores.size() > 0) {
				// find the label with the highest score from the ones received
				Double maxScore = -Double.MAX_VALUE;
				Long maxScoreLabel = vertex.getValue().f0;
				for (Long curLabel : receivedLabelsWithScores.keySet()) {
					if (receivedLabelsWithScores.get(curLabel) > maxScore) {
						maxScore = receivedLabelsWithScores.get(curLabel);
						maxScoreLabel = curLabel;
					}
				}
				// find the highest score of maxScoreLabel
				Double highestScore = labelsWithHighestScore.get(maxScoreLabel);
				// re-score the new label
				if (maxScoreLabel != vertex.getValue().f0) {
					highestScore -= delta / getSuperstepNumber();
				}
				// else delta = 0
				// update own label
				setNewVertexValue(new Tuple2<Long, Double>(maxScoreLabel, highestScore));
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class LabelMessenger extends MessagingFunction<String, Tuple2<Long, Double>,
			Tuple2<Long, Double>, Double> {
		@Override
		public void sendMessages(Vertex<String, Tuple2<Long, Double>> vertex) throws Exception {
			for(Edge<String, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), new Tuple2<Long, Double>(vertex.getValue().f0,
						vertex.getValue().f1 * edge.getValue()));
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class AddScoreToVertexValuesMapper implements MapFunction<Vertex<String, Long>, Tuple2<Long, Double>> {
		@Override
		public Tuple2<Long, Double> map(Vertex<String, Long> vertex) throws Exception {
			return new Tuple2<Long, Double>(vertex.getValue(), 1.0);
		}
	}

	@SuppressWarnings("serial")
	public static final class RemoveScoreFromVertexValuesMapper implements MapFunction<Vertex<String, Tuple2<Long, Double>>, Long> {
		@Override
		public Long map(Vertex<String, Tuple2<Long, Double>> vertex) throws Exception {
			return vertex.getValue().f0;
		}
	}
}