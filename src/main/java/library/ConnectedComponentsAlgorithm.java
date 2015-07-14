package library;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

/**
 * Connected components algorithm.
 *
 * Initially, each vertex will have its own ID as a value(is its own component). The vertices propagate their
 * current component ID in iterations, each time adopting a new value from the received neighbor IDs,
 * provided that the value is less than the current minimum.
 *
 * The algorithm converges when vertices no longer update their value or when the maximum number of iterations
 * is reached.
 */
@SuppressWarnings("serial")
public class ConnectedComponentsAlgorithm implements GraphAlgorithm<String, Long, NullValue> {

	private Integer maxIterations;

	public ConnectedComponentsAlgorithm(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<String, Long, NullValue> run(Graph<String, Long, NullValue> graph) throws Exception {

		Graph<String, Long, NullValue> undirectedGraph = graph.getUndirected();

		// initialize vertex values and run the Vertex Centric Iteration
		return undirectedGraph.runVertexCentricIteration(new CCUpdater(),
				new CCMessenger(), maxIterations);
	}

	/**
	 * Updates the value of a vertex by picking the minimum neighbor ID out of all the incoming messages.
	 */
	public static final class CCUpdater extends VertexUpdateFunction<String, Long, Long> {

		@Override
		public void updateVertex(Vertex<String, Long> vertex, MessageIterator<Long> messages) throws Exception {
			long min = Long.MAX_VALUE;

			for (long msg : messages) {
				min = Math.min(min, msg);
			}

			// update vertex value, if new minimum
			if (min < vertex.getValue()) {
				setNewVertexValue(min);
			}
		}
	}

	/**
	 * Distributes the minimum ID associated with a given vertex among all the target vertices.
	 */
	public static final class CCMessenger extends MessagingFunction<String, Long, Long, NullValue> {

		@Override
		public void sendMessages(Vertex<String, Long> vertex) throws Exception {
			// send current minimum to neighbors
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}
}
