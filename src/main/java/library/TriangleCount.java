package library;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

import java.util.HashSet;

public class TriangleCount implements GraphAlgorithm<Integer, HashSet<Integer>, NullValue> {

	private Integer maxIterations;

	public TriangleCount(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<Integer, HashSet<Integer>, NullValue> run(Graph<Integer, HashSet<Integer>, NullValue> graph) {

		Graph<Integer, HashSet<Integer>, NullValue> undirectedGraph = graph.getUndirected();

		VertexCentricIteration<Integer, HashSet<Integer>, Integer, NullValue> iteration = undirectedGraph
				.createVertexCentricIteration(new VertexUpdater(), new Messenger(), maxIterations);

		return undirectedGraph.runVertexCentricIteration(iteration);
	}

	public static final class VertexUpdater extends VertexUpdateFunction<Integer, HashSet<Integer>, Integer> {

		@Override
		public void updateVertex(Integer vertexKey, HashSet<Integer> neighborIds,
								 MessageIterator<Integer> inMessages) throws Exception {

			neighborIds.clear();
			Integer numberOfTriangles = 0;

			for(int msg : inMessages) {
				neighborIds.add(msg);

				if(getSuperstepNumber() == 3) {
					numberOfTriangles ++;
				}
			}

			if(getSuperstepNumber() == 3) {
				neighborIds.clear();

				// minus because if the vertex does not receive a message in the last step, it will not perform
				// a clear and the values will not be changed. We subtract the values we had at a previous step
				// in order to only retain the valid ones
				neighborIds.add(-numberOfTriangles);
			}
			setNewVertexValue(neighborIds);
		}
	}

	public static final class Messenger extends MessagingFunction<Integer, HashSet<Integer>, Integer, NullValue> {

		@Override
		public void sendMessages(Integer vertexKey, HashSet<Integer> vertexValue) throws Exception {
			Iterable<Edge<Integer, NullValue>> edges = getOutgoingEdges();

			if(getSuperstepNumber() == 1) {
				for(Edge<Integer, NullValue> edge : edges) {
					if(vertexKey < edge.getTarget()) {
						sendMessageTo(edge.getTarget(), vertexKey);
					}
				}
			}

			if(getSuperstepNumber() == 2) {
				for(Edge<Integer, NullValue> edge : edges) {
					 for(int neighborId : vertexValue) {
						if((neighborId < vertexKey)  && (vertexKey < edge.getTarget())) {
							sendMessageTo(edge.getTarget(), neighborId);
						}
					}
				}
			}

			if(getSuperstepNumber() == 3) {
				for(Edge<Integer, NullValue> edge : edges) {
					for(int neighborId : vertexValue) {
						if(neighborId == edge.getTarget()) {
							sendMessageTo(edge.getTarget(), neighborId);
						}
					}
				}
			}
		}
	}
}
