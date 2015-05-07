package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunction;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import util.JaccardSimilarityMeasureData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class JaccardSimilarityMeasure implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		// the result is stored within the vertex value
		DataSet<Vertex<Long, Tuple2<HashSet<Long>,HashMap<Long, Double>>>> verticesWithNeighbors =
				graph.groupReduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);

		Graph<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue> graphWithVertexValues =
				Graph.fromDataSet(verticesWithNeighbors, edges, env);

		Graph<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue> undirectedGraph =
				graphWithVertexValues.getUndirected();

		// simulate a vertex centric iteration with groupReduceOnNeighbors
		DataSet<Vertex<Long, HashMap<Long, Double>>> verticesWithJaccardValues =
				undirectedGraph.groupReduceOnNeighbors(new ComputeJaccard(), EdgeDirection.ALL);

		// emit result
		if (fileOutput) {
			verticesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");
		} else {
			verticesWithJaccardValues.print();
		}

		env.execute("Executing Jaccard Similarity Measure");
	}


	@Override
	public String getDescription() {
		return "Vertex Jaccard Similarity Measure";
	}

	/**
	 * Each vertex will have a HashSet containing its neighbors as value.
	 */
	@SuppressWarnings("serial")
	private static final class GatherNeighbors implements NeighborsFunction<Long, NullValue, NullValue,
			Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>> {

		@Override
		public void iterateNeighbors(Iterable<Tuple3<Long, Edge<Long, NullValue>, Vertex<Long, NullValue>>> neighbors,
									 Collector<Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>> collector) throws Exception {

			HashSet<Long> neighborsHashSet = new HashSet<Long>();
			Tuple3<Long, Edge<Long, NullValue>, Vertex<Long, NullValue>> next = null;
			Iterator<Tuple3<Long, Edge<Long, NullValue>, Vertex<Long, NullValue>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				neighborsHashSet.add(next.f2.getId());
			}

			collector.collect(new Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>(next.f0,
					new Tuple2<>(neighborsHashSet, new HashMap<Long, Double>())));
		}
	}

	/**
	 * Each vertex will have a HashMap containing the Jaccard coefficient for each of its values.
	 *
	 * Consider the edge x-y
	 * We denote by sizeX and sizeY, the neighbors hash set size of x and y respectively.
	 * sizeX+sizeY = union + intersection of neighborhoods
	 * size(hashSetX.addAll(hashSetY)).distinct = union of neighborhoods
	 * The intersection can then be deduced.
	 *
	 * Jaccard Similarity is then, the intersection/union.
	 */
	@SuppressWarnings("serial")
	private static final class ComputeJaccard implements NeighborsFunctionWithVertexValue<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue,
				Vertex<Long, HashMap<Long, Double>>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>> vertex,
									 Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>>> neighbors,
									 Collector<Vertex<Long, HashMap<Long, Double>>> collector) throws Exception {

			HashMap<Long, Double> jaccard = new HashMap<>();
			Tuple2<Edge<Long, NullValue>, Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>> next = null;
			Iterator<Tuple2<Edge<Long, NullValue>, Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>> neighborVertex = next.f1;

				Long y = neighborVertex.getId();
				HashSet<Long> neighborSetY = neighborVertex.getValue().f0;

				double unionPlusIntersection = vertex.getValue().f0.size() + neighborSetY.size();
				// within a HashSet, all elements are distinct
				HashSet<Long> unionSet = new HashSet<>();
				unionSet.addAll(vertex.getValue().f0);
				unionSet.addAll(neighborSetY);
				double union = unionSet.size();
				double intersection = unionPlusIntersection - union;

				jaccard.put(y, intersection / union);
			}

			collector.collect(new Vertex<Long, HashMap<Long, Double>>(vertex.getId(), jaccard));
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = JaccardSimilarityMeasureData.MAX_ITERATIONS;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage JaccardSimilarityMeasure <edge path> <output path> <maxIterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing JaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage JaccardSimilarityMeasure <edge path> <output path> <maxIterations>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<Long, NullValue>(tuple2.f0, tuple2.f1, NullValue.getInstance());
						}
					});
		} else {
			return JaccardSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}
