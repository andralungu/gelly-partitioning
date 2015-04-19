package library;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

public class CountDegree implements GraphAlgorithm<String, Long, NullValue> {

	private Integer maxIterations;

	public CountDegree(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<String, Long, NullValue> run(Graph<String, Long, NullValue> graph) throws Exception {

		Graph<String, Long, NullValue> undirectedGraph = graph.getUndirected();

		return undirectedGraph.runVertexCentricIteration(new VertexUpdater(), new Messenger(), maxIterations);
	}

	/**
	 * Each vertex will count the number of ones it received from its neighbors and set the total as a value.
	 */
	public static final class VertexUpdater extends VertexUpdateFunction<String, Long, Long> {

		@Override
		public void updateVertex(String vertexKey, Long vertexValue,
								 MessageIterator<Long> inMessages) throws Exception {

			long degree = 0L;

			for(long msg : inMessages) {
				degree += msg;
			}

			setNewVertexValue(degree);
		}
	}

	/**
	 * Each vertex will send 1L to its neighbors.
	 */
	public static final class Messenger extends MessagingFunction<String, Long, Long, NullValue> {

		@Override
		public void sendMessages(String vertexKey, Long vertexValue) throws Exception {
			for(Edge<String, NullValue> edge : getOutgoingEdges()) {
				sendMessageTo(edge.getTarget(), 1L);
			}
		}
	}
}
