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

public class GSAJaccardSimilarityMeasure implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		// initialize the vertex values with empty sets
		Graph<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue> graph = Graph.fromDataSet(edges,
				new MapFunction<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>() {

					@Override
					public Tuple2<HashSet<Long>, HashMap<Long, Double>> map(Long id) throws Exception {
						HashSet<Long> neighbors = new HashSet<Long>();
						neighbors.add(id);

						return new Tuple2<HashSet<Long>, HashMap<Long, Double>>(neighbors,
								new HashMap<Long, Double>());
					}
				}, env);

		// Simulate GSA
		// Gather: no-op in this case
		// Sum: create the set of neighbors
		DataSet<Tuple2<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>> computedNeighbors =
				graph.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);

		// Apply: attach the computed values as vertex values
		DataSet<Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>> verticesWithNeighbors =
				computedNeighbors.map(new MapFunction<Tuple2<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>, Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>>() {

					@Override
					public Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>> map(Tuple2<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>> tuple2) throws Exception {
						return new Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>(tuple2.f0, tuple2.f1);
					}
				});

		Graph<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue> graphWithNeighbors =
				Graph.fromDataSet(verticesWithNeighbors, edges, env);

		// Scatter: compare neighbors; compute Jaccard
		DataSet<Vertex<Long, HashMap<Long, Double>>> verticesWithJaccardValues =
				graphWithNeighbors.getTriplets().flatMap(new ComputeJaccard())
						.groupBy(0).reduce(new ReduceFunction<Vertex<Long, HashMap<Long, Double>>>() {

					@Override
					public Vertex<Long, HashMap<Long, Double>> reduce(Vertex<Long, HashMap<Long, Double>> first,
																	  Vertex<Long, HashMap<Long, Double>> second) throws Exception {
						first.getValue().putAll(second.getValue());
						return new Vertex<Long, HashMap<Long, Double>>(first.getId(), first.getValue());
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
	private static final class GatherNeighbors implements ReduceNeighborsFunction<Tuple2<HashSet<Long>, HashMap<Long, Double>>> {

		@Override
		public Tuple2<HashSet<Long>, HashMap<Long, Double>> reduceNeighbors(Tuple2<HashSet<Long>, HashMap<Long, Double>> first,
																			Tuple2<HashSet<Long>, HashMap<Long, Double>> second) {
			first.f0.addAll(second.f0);
			return new Tuple2<HashSet<Long>, HashMap<Long, Double>>(first.f0, new HashMap<Long, Double>());
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
			FlatMapFunction<Triplet<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue>, Vertex<Long, HashMap<Long, Double>>> {

		@Override
		public void flatMap(Triplet<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>, NullValue> triplet,
							Collector<Vertex<Long, HashMap<Long, Double>>> collector) throws Exception {

			Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>> srcVertex = triplet.getSrcVertex();
			Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>> trgVertex = triplet.getTrgVertex();

			HashMap<Long, Double> jaccard = new HashMap<Long, Double>();
			HashMap<Long, Double> jaccardReversed = new HashMap<Long, Double>();

			Long x = srcVertex.getId();
			Long y = trgVertex.getId();
			HashSet<Long> neighborSetY = trgVertex.getValue().f0;

			double unionPlusIntersection = srcVertex.getValue().f0.size() + neighborSetY.size();
			// within a HashSet, all elements are distinct
			HashSet<Long> unionSet = new HashSet<>();
			unionSet.addAll(srcVertex.getValue().f0);
			unionSet.addAll(neighborSetY);
			double union = unionSet.size();
			double intersection = unionPlusIntersection - union;

			jaccard.put(y, intersection / union);
			jaccardReversed.put(x, intersection/union);

			collector.collect(new Vertex<Long, HashMap<Long, Double>>(srcVertex.getId(), jaccard));
			collector.collect(new Vertex<Long, HashMap<Long, Double>>(trgVertex.getId(), jaccardReversed));
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
