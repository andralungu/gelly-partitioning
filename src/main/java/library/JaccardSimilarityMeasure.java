package library;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

import java.io.File;
import java.io.IOException;
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

		private File tempFile;

		public VertexUpdateJaccardCoefficient() {
			try {
				tempFile = File.createTempFile("update_monitoring", ".txt");
				System.out.println("Vertices file" + tempFile.getAbsolutePath());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void updateVertex(Long vertexKey, Tuple2<HashSet<Long>, HashMap<Long,Double>> vertexValue,
								 MessageIterator<Tuple2<Long, HashSet<Long>>> inMessages) throws Exception {

			long start = System.currentTimeMillis();

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

			long stop = System.currentTimeMillis();
			long time = stop - start;
			String updateTimeElapsed = "Vertex key " + vertexKey +" Superstep number " + getSuperstepNumber() +
					" time elapsed vertex update " + time + "\n";
			Files.append(updateTimeElapsed, tempFile, Charsets.UTF_8);
		}
	}

	/**
	 * Each vertex sends out its set of neighbors.
	 */
	public static final class Messenger extends MessagingFunction<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>,
			Tuple2<Long, HashSet<Long>>, NullValue> {

		private File tempFile;

		public Messenger() {
			try {
				tempFile = File.createTempFile("message_monitoring", ".txt");
				System.out.println("Messages file" + tempFile.getAbsolutePath());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void sendMessages(Long vertexKey, Tuple2<HashSet<Long>, HashMap<Long,Double>> vertexValue) throws Exception {

			long start = System.currentTimeMillis();

			int numberOfMessages = 0;

			if(getSuperstepNumber() == 1) {
				for (Edge<Long, NullValue> edge : getOutgoingEdges()) {
					sendMessageTo(edge.getTarget(), new Tuple2<Long, HashSet<Long>>(vertexKey, vertexValue.f0));
					numberOfMessages++;
				}

				String messages = "Vertex key " + vertexKey + " Superstep number " + getSuperstepNumber() +
						" number of messages " + numberOfMessages + "\n";
				Files.append(messages, tempFile, Charsets.UTF_8);
			}

			long stop = System.currentTimeMillis();
			long time = stop - start;
			String updateTimeElapsed = "Vertex key " + vertexKey + " Superstep number " + getSuperstepNumber() +
					" time elapsed messaging function " + time + "\n";
			Files.append(updateTimeElapsed, tempFile, Charsets.UTF_8);
		}
	}
}
