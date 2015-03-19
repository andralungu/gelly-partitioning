package library;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

import java.util.HashMap;
import java.util.HashSet;

public class JaccardSimilarityMeasure implements GraphAlgorithm<Long, Tuple2<HashSet<Long>,HashMap<Long,Double>>, NullValue> {

	private Integer maxIterations;

	public JaccardSimilarityMeasure(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue> run(
			Graph<Long, Tuple2<HashSet<Long>, HashMap<Long,Double>>, NullValue> graph) {

		Graph<Long, Tuple2<HashSet<Long>, HashMap<Long,Double>>, NullValue> undirectedGraph = graph.getUndirected();

		VertexCentricIteration<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, Tuple2<Long, HashSet<Long>>, NullValue> iteration =
				undirectedGraph.createVertexCentricIteration(new VertexUpdateJaccardCoefficient(), new Messenger(), maxIterations);

		return undirectedGraph.runVertexCentricIteration(iteration);
	}

	/**
	 * Consider the edge x-y
	 * We denote by sizeX and sizeY, the neighbors hash set size of x and y respectively.
	 * sizeX+sizeY = union + intersection of neighborhoods
	 * size(hashSetX.addAll(hashSetY)).distinct = union of neighborhoods
	 * The intersection can then be deduced.
	 *
	 * Jaccard Similarity is then, the intersection/union.
	 */
	public static final class VertexUpdateJaccardCoefficient extends VertexUpdateFunction<Long,
			Tuple2<HashSet<Long>, HashMap<Long,Double>>, Tuple2<Long, HashSet<Long>>> {

		@Override
		public void updateVertex(Long vertexKey, Tuple2<HashSet<Long>, HashMap<Long,Double>> vertexValue,
								 MessageIterator<Tuple2<Long, HashSet<Long>>> inMessages) throws Exception {

			HashMap<Long, Double> jaccard = new HashMap<>();

			for(Tuple2<Long, HashSet<Long>> msg : inMessages) {

				Long y = msg.f0;
				HashSet<Long> neighborSetY = msg.f1;

				double unionPlusIntersection = vertexValue.f0.size() + neighborSetY.size();
				// within a HashSet, all elements are distinct
				HashSet<Long> unionSet = new HashSet<>();
				unionSet.addAll(vertexValue.f0);
				unionSet.addAll(neighborSetY);
				double union = unionSet.size();
				double intersection = unionPlusIntersection - union;

				jaccard.put(y, intersection / union);
			}

			setNewVertexValue(new Tuple2<HashSet<Long>, HashMap<Long, Double>>(vertexValue.f0, jaccard));
		}
	}

	/**
	 * Each vertex sends out its set of neighbors.
	 */
	public static final class Messenger extends MessagingFunction<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>,
			Tuple2<Long, HashSet<Long>>, NullValue> {

		@Override
		public void sendMessages(Long vertexKey, Tuple2<HashSet<Long>, HashMap<Long,Double>> vertexValue) throws Exception {
			if(getSuperstepNumber() == 1) {
				for (Edge<Long, NullValue> edge : getOutgoingEdges()) {
					sendMessageTo(edge.getTarget(), new Tuple2<Long, HashSet<Long>>(vertexKey, vertexValue.f0));
				}
			}
		}
	}
}
