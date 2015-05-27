package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import util.JaccardSimilarityMeasureData;

import java.util.HashSet;

public class GSAJaccardSimilarityMeasure implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		// initialize the vertex values with empty sets
		Graph<String, HashSet<String>, NullValue> graph = Graph.fromDataSet(edges,
				new MapFunction<String, HashSet<String>>() {

					@Override
					public HashSet<String> map(String id) throws Exception {
						HashSet<String> neighbors = new HashSet<String>();
						neighbors.add(id);

						return new HashSet<String>(neighbors);
					}
				}, env);

		// Simulate GSA
		// Gather: no-op in this case
		// Sum: create the set of neighbors
		DataSet<Tuple2<String, HashSet<String>>> computedNeighbors =
				graph.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);

		// Apply: attach the computed values to the vertices
		// joinWithVertices to update the node values
		DataSet<Vertex<String, HashSet<String>>> verticesWithNeighbors =
				graph.joinWithVertices(computedNeighbors, new MapFunction<Tuple2<HashSet<String>, HashSet<String>>,
						HashSet<String>>() {

					@Override
					public HashSet<String> map(Tuple2<HashSet<String>, HashSet<String>> tuple2) throws Exception {
						return tuple2.f1;
					}
				}).getVertices();

		Graph<String, HashSet<String>, NullValue> graphWithNeighbors =
				Graph.fromDataSet(verticesWithNeighbors, edges, env);

		// Scatter: compare neighbors; compute Jaccard
		DataSet<Edge<String, Double>> edgesWithJaccardValues = graphWithNeighbors.getTriplets()
				.map(new ComputeJaccard());

		// emit result
		if (fileOutput) {
			edgesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");
		} else {
			edgesWithJaccardValues.print();
		}

		env.execute("Executing GSA Jaccard Similarity Measure");
	}

	@Override
	public String getDescription() {
		return "GSA Jaccard Similarity Measure";
	}

	/**
	 * Each vertex will have a HashSet containing its neighbors as value.
	 */
	@SuppressWarnings("serial")
	private static final class GatherNeighbors implements ReduceNeighborsFunction<HashSet<String>> {

		@Override
		public HashSet<String> reduceNeighbors(HashSet<String> first,
											   HashSet<String> second) {
			first.addAll(second);
			return new HashSet<String>(first);
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
	private static final class ComputeJaccard implements
			MapFunction<Triplet<String, HashSet<String>, NullValue>, Edge<String, Double>> {

		@Override
		public Edge<String, Double> map(Triplet<String, HashSet<String>, NullValue> triplet) throws Exception {

			Vertex<String, HashSet<String>> srcVertex = triplet.getSrcVertex();
			Vertex<String, HashSet<String>> trgVertex = triplet.getTrgVertex();

			String x = srcVertex.getId();
			String y = trgVertex.getId();
			HashSet<String> neighborSetY = trgVertex.getValue();

			double unionPlusIntersection = srcVertex.getValue().size() + neighborSetY.size();
			// within a HashSet, all elements are distinct
			HashSet<String> unionSet = new HashSet<String>();
			unionSet.addAll(srcVertex.getValue());
			unionSet.addAll(neighborSetY);
			double union = unionSet.size();
			double intersection = unionPlusIntersection - union;

			return new Edge<String, Double>(x, y, intersection/union);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage JaccardSimilarityMeasure <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing JaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage JaccardSimilarityMeasure <edge path> <output path>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<String, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(String.class, String.class)
					.map(new MapFunction<Tuple2<String, String>, Edge<String, NullValue>>() {
						@Override
						public Edge<String, NullValue> map(Tuple2<String, String> tuple2) throws Exception {
							return new Edge<String, NullValue>(tuple2.f0, tuple2.f1, NullValue.getInstance());
						}
					});
		} else {
			return JaccardSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}
