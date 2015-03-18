package library;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

import java.util.Map;
import java.util.TreeMap;

public class SimpleCommunityDetection implements GraphAlgorithm<Long, Tuple2<Long, Double>, Double> {

	private Integer maxIterations;

	public SimpleCommunityDetection(Integer maxIterations) {

		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<Long, Tuple2<Long, Double>, Double> run(Graph<Long, Tuple2<Long, Double>, Double> graph) {

		Graph<Long, Tuple2<Long, Double>, Double> undirectedGraph = graph.getUndirected();

		VertexCentricIteration <Long, Tuple2<Long, Double>, Tuple2<Long, Double>, Double>
				iteration = undirectedGraph.createVertexCentricIteration(new VertexLabelUpdater(),
				new LabelMessenger(), maxIterations);

		return undirectedGraph.runVertexCentricIteration(iteration);
	}

	public static final class VertexLabelUpdater extends VertexUpdateFunction<Long, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public void updateVertex(Long vertexKey, Tuple2<Long, Double> labelScore,
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
				Long maxScoreLabel = labelScore.f0;
				for (Long curLabel : receivedLabelsWithScores.keySet()) {

					if (receivedLabelsWithScores.get(curLabel) > maxScore) {
						maxScore = receivedLabelsWithScores.get(curLabel);
						maxScoreLabel = curLabel;
					}
				}

				// find the highest score of maxScoreLabel
				Double highestScore = labelsWithHighestScore.get(maxScoreLabel);
				// re-score the new label
				if (maxScoreLabel != labelScore.f0) {
					// delta = 0.5
					highestScore -= 0.5f / getSuperstepNumber();
				}
				// else delta = 0
				// update own label
				setNewVertexValue(new Tuple2<Long, Double>(maxScoreLabel, highestScore));
			}
		}
	}

	public static final class LabelMessenger extends MessagingFunction<Long, Tuple2<Long, Double>,
			Tuple2<Long, Double>, Double> {

		@Override
		public void sendMessages(Long vertexKey, Tuple2<Long, Double> vertexValue) throws Exception {

			for(Edge<Long, Double> edge : getOutgoingEdges()) {
				sendMessageTo(edge.getTarget(), new Tuple2<Long, Double>(vertexValue.f0, vertexValue.f1 * edge.getValue()));
			}

		}
	}
}
