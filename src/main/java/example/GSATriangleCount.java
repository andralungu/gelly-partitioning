package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import util.TriangleCountData;

import java.util.HashSet;

public class GSATriangleCount implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		// initialize the vertex values with hash sets containing their own ids
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

		// superstep 1
		// Gather: a no-op in this case
		// Sum: compute the set of neighbors with id lower than your own
		// select the edges with src < trg
		DataSet<Edge<String, NullValue>> edgesWithSrcLowerThanTrg =
				edges.map(new MapFunction<Edge<String, NullValue>, Edge<String, NullValue>>() {

					@Override
					public Edge<String, NullValue> map(Edge<String, NullValue> edge) throws Exception {
						if(Long.parseLong(edge.getSource()) < Long.parseLong(edge.getTarget())) {
							return edge;
						} else {
							return new Edge<String, NullValue>(edge.getTarget(), edge.getSource(), NullValue.getInstance());
						}
					}
				}).distinct();

		Graph<String, HashSet<String>, NullValue> graphWithSrcLowerThanTrg =
				Graph.fromDataSet(graph.getVertices(), edgesWithSrcLowerThanTrg, env);
		// compute the neighbors
		DataSet<Tuple2<String, HashSet<String>>> computedNeighbors =
				graphWithSrcLowerThanTrg.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.IN);

		// Apply: attach values to vertices
		Graph<String, HashSet<String>, NullValue> graphAfterTheFirstSuperstep =
				graphWithSrcLowerThanTrg
						.mapVertices(new MapFunction<Vertex<String, HashSet<String>>, HashSet<String>>() {
							@Override
							public HashSet<String> map(Vertex<String, HashSet<String>> vertex) throws Exception {
								return new HashSet<String>();
							}
						})
						.joinWithVertices(computedNeighbors, new MapFunction<Tuple2<HashSet<String>, HashSet<String>>,
								HashSet<String>>() {

							@Override
							public HashSet<String> map(Tuple2<HashSet<String>, HashSet<String>> tuple2) throws Exception {
								return tuple2.f1;
							}
						});

		// superstep 2
		// Sum: retrieve lower id neighbors
		DataSet<Tuple2<String, HashSet<String>>> computedNeighborsSuperstep2 =
				graphAfterTheFirstSuperstep.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.IN);

		// Apply: attach values to vertices
		Graph<String, HashSet<String>, NullValue> graphAfterTheSecondSuperstep =
				graphAfterTheFirstSuperstep
						.mapVertices(new MapFunction<Vertex<String, HashSet<String>>, HashSet<String>>() {
							@Override
							public HashSet<String> map(Vertex<String, HashSet<String>> stringHashSetVertex) throws Exception {
								return new HashSet<String>();
							}
						})
						.joinWithVertices(computedNeighborsSuperstep2, new MapFunction<Tuple2<HashSet<String>, HashSet<String>>,
								HashSet<String>>() {

							@Override
							public HashSet<String> map(Tuple2<HashSet<String>, HashSet<String>> tuple2) throws Exception {
								return tuple2.f1;
							}
						});

		// Scatter: count the total number of triangles
		DataSet<Tuple1<Integer>> numberOfTriangles = graphAfterTheSecondSuperstep.getTriplets()
				.map(new CountTriangles())
				.reduce(new ReduceFunction<Tuple1<Integer>>() {
					@Override
					public Tuple1<Integer> reduce(Tuple1<Integer> firstTuple, Tuple1<Integer> secondTuple) throws Exception {
						return new Tuple1<Integer>(firstTuple.f0 + secondTuple.f0);
					}
				});

		// emit result
		if(fileOutput) {
			numberOfTriangles.writeAsCsv(outputPath, "\n", ",");
			env.execute("Executing GSA Triangle Count");
		} else {
			numberOfTriangles.print();
		}
	}

	@Override
	public String getDescription() {
		return "GSA Triangle Count";
	}

	/**
	 * Each vertex will have a HashSet containing its lower id neighbors as value.
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

	@SuppressWarnings("serial")
	private static final class CountTriangles implements MapFunction<Triplet<String, HashSet<String>, NullValue>, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Triplet<String, HashSet<String>, NullValue> triplet) throws Exception {
			Vertex<String, HashSet<String>> src = triplet.getSrcVertex();
			Vertex<String, HashSet<String>> trg = triplet.getTrgVertex();
			int numberOfTriangles = 0;

			if(trg.getValue().contains(src.getId())) {
				numberOfTriangles++;
			}
			return new Tuple1<Integer>(numberOfTriangles);
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
				System.err.println("Usage GSATriangleCount <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing GSATriangleCount example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage GSATriangleCount <edge path> <output path>");
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
			return TriangleCountData.getDefaultEdgeDataSet(env);
		}
	}
}
