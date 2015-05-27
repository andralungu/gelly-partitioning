package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.util.Collector;
import util.JaccardSimilarityMeasureData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

public class GSAJaccardSimilarityMeasure implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		// initialize the vertex values with empty sets
		Graph<String, Tuple2<HashSet<String>, HashMap<String, Double>>, NullValue> graph = Graph.fromDataSet(edges,
				new MapFunction<String, Tuple2<HashSet<String>, HashMap<String, Double>>>() {

					@Override
					public Tuple2<HashSet<String>, HashMap<String, Double>> map(String id) throws Exception {
						HashSet<String> neighbors = new HashSet<String>();
						neighbors.add(id);

						return new Tuple2<HashSet<String>, HashMap<String, Double>>(neighbors,
								new HashMap<String, Double>());
					}
				}, env);

		// Simulate GSA
		// Gather: no-op in this case
		// Sum: create the set of neighbors
		DataSet<Tuple2<String, Tuple2<HashSet<String>, HashMap<String, Double>>>> computedNeighbors =
				graph.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);

		// Apply: attach the computed values as vertex values
		DataSet<Vertex<String, Tuple2<HashSet<String>, HashMap<String, Double>>>> verticesWithNeighbors =
				computedNeighbors.map(new MapFunction<Tuple2<String, Tuple2<HashSet<String>, HashMap<String, Double>>>,
						Vertex<String, Tuple2<HashSet<String>, HashMap<String, Double>>>>() {

					@Override
					public Vertex<String, Tuple2<HashSet<String>, HashMap<String, Double>>> map(Tuple2<String, Tuple2<HashSet<String>,
							HashMap<String, Double>>> tuple2) throws Exception {

						return new Vertex<String, Tuple2<HashSet<String>, HashMap<String, Double>>>(tuple2.f0, tuple2.f1);
					}
				});

		Graph<String, Tuple2<HashSet<String>, HashMap<String, Double>>, NullValue> graphWithNeighbors =
				Graph.fromDataSet(verticesWithNeighbors, edges, env);

		// Scatter: compare neighbors; compute Jaccard
		DataSet<Vertex<String, TreeMap<String, Double>>> verticesWithJaccardValues =
				graphWithNeighbors.getTriplets().flatMap(new ComputeJaccard())
						.groupBy(0).reduce(new ReduceFunction<Vertex<String, TreeMap<String, Double>>>() {

					@Override
					public Vertex<String, TreeMap<String, Double>> reduce(Vertex<String, TreeMap<String, Double>> first,
																	  Vertex<String, TreeMap<String, Double>> second) throws Exception {
						first.getValue().putAll(second.getValue());
						return new Vertex<String, TreeMap<String, Double>>(first.getId(), first.getValue());
					}
				});

		// emit result
		if (fileOutput) {
			verticesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");
		} else {
			verticesWithJaccardValues.print();
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
	private static final class GatherNeighbors implements ReduceNeighborsFunction<Tuple2<HashSet<String>, HashMap<String, Double>>> {

		@Override
		public Tuple2<HashSet<String>, HashMap<String, Double>> reduceNeighbors(Tuple2<HashSet<String>, HashMap<String, Double>> first,
																			Tuple2<HashSet<String>, HashMap<String, Double>> second) {
			first.f0.addAll(second.f0);
			return new Tuple2<HashSet<String>, HashMap<String, Double>>(first.f0, new HashMap<String, Double>());
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
			FlatMapFunction<Triplet<String, Tuple2<HashSet<String>, HashMap<String, Double>>, NullValue>,
					Vertex<String, TreeMap<String, Double>>> {

		@Override
		public void flatMap(Triplet<String, Tuple2<HashSet<String>, HashMap<String, Double>>, NullValue> triplet,
							Collector<Vertex<String, TreeMap<String, Double>>> collector) throws Exception {

			Vertex<String, Tuple2<HashSet<String>, HashMap<String, Double>>> srcVertex = triplet.getSrcVertex();
			Vertex<String, Tuple2<HashSet<String>, HashMap<String, Double>>> trgVertex = triplet.getTrgVertex();

			TreeMap<String, Double> jaccard = new TreeMap<String, Double>();
			TreeMap<String, Double> jaccardReversed = new TreeMap<String, Double>();

			String x = srcVertex.getId();
			String y = trgVertex.getId();
			HashSet<String> neighborSetY = trgVertex.getValue().f0;

			double unionPlusIntersection = srcVertex.getValue().f0.size() + neighborSetY.size();
			// within a HashSet, all elements are distinct
			HashSet<String> unionSet = new HashSet<String>();
			unionSet.addAll(srcVertex.getValue().f0);
			unionSet.addAll(neighborSetY);
			double union = unionSet.size();
			double intersection = unionPlusIntersection - union;

			jaccard.put(y, intersection / union);
			jaccardReversed.put(x, intersection/union);

			collector.collect(new Vertex<String, TreeMap<String, Double>>(srcVertex.getId(), jaccard));
			collector.collect(new Vertex<String, TreeMap<String, Double>>(trgVertex.getId(), jaccardReversed));
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
