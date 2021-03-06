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
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import util.DummyGraph;
import util.TriangleCountData;

import java.io.File;
import java.util.TreeMap;

public class GSATriangleCount implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// order the edges so that src is always higher than trg
		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env)
				.map(new MapFunction<Edge<String, NullValue>, Edge<String, NullValue>>() {

					@Override
					public Edge<String, NullValue> map(Edge<String, NullValue> edge) throws Exception {
						if (Long.parseLong(edge.getSource()) < Long.parseLong(edge.getTarget())) {
							return new Edge<String, NullValue>(edge.getTarget(), edge.getSource(), NullValue.getInstance());
						} else {
							return edge;
						}
					}
				}).distinct();

		DummyGraph<String, TreeMap<String, Integer>, NullValue> graph = DummyGraph.fromDataSet(edges,
				new MapFunction<String, TreeMap<String, Integer>>() {

					@Override
					public TreeMap<String, Integer> map(String s) throws Exception {
						TreeMap<String, Integer> neighbors = new TreeMap<String, Integer>();
						neighbors.put(s, 1);

						return neighbors;
					}
				}, env);

		computationTempFile = File.createTempFile("computation_monitoring", ".txt");
		System.out.println("Computation file" + computationTempFile.getAbsolutePath());

		// select neighbors with ids higher than the current vertex id
		// Gather: a no-op in this case
		// Sum: create the set of neighbors
		DataSet<Tuple2<String, TreeMap<String, Integer>>> higherIdNeighbors =
				graph.reduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN, computationTempFile);

		DummyGraph<String, TreeMap<String, Integer>, NullValue> graphWithReinitializedVertexValues =
				graph.mapVertices(new MapFunction<Vertex<String, TreeMap<String, Integer>>, TreeMap<String, Integer>>() {
					@Override
					public TreeMap<String, Integer> map(Vertex<String, TreeMap<String, Integer>> stringArrayListVertex) throws Exception {
						return new TreeMap<String, Integer>();
					}
				});

		// Apply: attach the computed values to the vertices
		// joinWithVertices to update the node values
		DataSet<Vertex<String, TreeMap<String, Integer>>> verticesWithHigherIdNeighbors =
				graphWithReinitializedVertexValues.joinWithVerticesTuple2(higherIdNeighbors, new MapFunction<Tuple2<TreeMap<String, Integer>,
						TreeMap<String, Integer>>, TreeMap< String, Integer >> () {
					@Override
					public TreeMap<String, Integer> map(Tuple2<TreeMap<String, Integer>, TreeMap<String, Integer>> tuple2) throws Exception {
						return tuple2.f1;
					}
				}).getVertices();

		DummyGraph<String, TreeMap<String,Integer>, NullValue> graphWithNeighbors = DummyGraph.fromDataSet(verticesWithHigherIdNeighbors,
				edges, env);

		// propagate each received value to neighbors with higher id
		// Gather: a no-op in this case
		// Sum: propagate values
		DataSet<Tuple2<String, TreeMap<String, Integer>>> propagatedValues = graphWithNeighbors
				.reduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN, computationTempFile);

		// Apply: attach propagated values to vertices
		DataSet<Vertex<String, TreeMap<String, Integer>>> verticesWithPropagatedValues =
				graphWithReinitializedVertexValues.joinWithVerticesTuple2(propagatedValues, new MapFunction<Tuple2<TreeMap<String, Integer>,
						TreeMap<String, Integer>>, TreeMap<String, Integer>>() {
					@Override
					public TreeMap<String, Integer> map(Tuple2<TreeMap<String, Integer>, TreeMap<String, Integer>> tuple2) throws Exception {
						return tuple2.f1;
					}
				}).getVertices();

		DummyGraph<String, TreeMap<String, Integer>, NullValue> graphWithPropagatedNeighbors =
				DummyGraph.fromDataSet(verticesWithPropagatedValues, graphWithNeighbors.getEdges(), env);

		// Scatter: compute the number of triangles
		DataSet<Tuple1<Integer>> numberOfTriangles = graphWithPropagatedNeighbors.getTriplets()
				.map(new ComputeTriangles()).reduce(new ReduceFunction<Tuple1<Integer>>() {

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

	@SuppressWarnings("serial")
	private static final class GatherHigherIdNeighbors implements ReduceNeighborsFunction<TreeMap<String,Integer>> {

		@Override
		public TreeMap<String,Integer> reduceNeighbors(TreeMap<String,Integer> first, TreeMap<String,Integer> second) {
			for (String key : second.keySet()) {
				Integer value = first.get(key);
				if (value != null) {
					first.put(key, value + second.get(key));
				} else {
					first.put(key, second.get(key));
				}
			}
			return first;
		}
	}

	@SuppressWarnings("serial")
	private static final class ComputeTriangles implements MapFunction<Triplet<String, TreeMap<String, Integer>, NullValue>, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Triplet<String, TreeMap<String, Integer>, NullValue> triplet) throws Exception {

			Vertex<String, TreeMap<String, Integer>> srcVertex = triplet.getSrcVertex();
			Vertex<String, TreeMap<String, Integer>> trgVertex = triplet.getTrgVertex();
			int triangles = 0;

			if(trgVertex.getValue().get(srcVertex.getId()) != null) {
				triangles=trgVertex.getValue().get(srcVertex.getId());
			}
			return new Tuple1<Integer>(triangles);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static File computationTempFile;

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
