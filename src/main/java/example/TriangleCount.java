package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import util.DummyGraph;
import util.FileAppendSingleton;
import util.TriangleCountData;

import java.io.File;
import java.util.Iterator;
import java.util.TreeMap;

public class TriangleCount implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		DummyGraph<String, NullValue, NullValue> graph = DummyGraph.fromDataSet(edges, env).getUndirected();

		// simulate the first superstep
		// select the neighbors with id greater than the current vertex's id
		DataSet<Vertex<String, String>> verticesWithHigherNeighbors =
				graph.groupReduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<String, TreeMap<String,Integer>>> verticesWithNeighborTreeMaps =
				verticesWithHigherNeighbors.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		// you want to assign a value to the vertices with no neighbors as well
		DummyGraph<String, TreeMap<String, Integer>, NullValue> graphWithInitializedVertexNeighbors =
				graph.mapVertices(new MapFunction<Vertex<String, NullValue>, TreeMap<String, Integer>>() {
					@Override
					public TreeMap<String, Integer> map(Vertex<String, NullValue> vertex) throws Exception {
						return new TreeMap<String, Integer>();
					}
				});

		DummyGraph<String, TreeMap<String, Integer>, NullValue> graphWithVertexNeighbors = graphWithInitializedVertexNeighbors.
				joinWithVertices(verticesWithNeighborTreeMaps, new MapFunction<Vertex<TreeMap<String, Integer>, TreeMap<String, Integer>>, TreeMap<String, Integer>>() {
					@Override
					public TreeMap<String, Integer> map(Vertex<TreeMap<String, Integer>, TreeMap<String, Integer>> vertex) throws Exception {
						return vertex.getValue();
					}
				});

		// simulate the second superstep
		// propagate each received "message" to neighbours with higher id
		DataSet<Vertex<String, String>> verticesWithPropagatedValues =
				graphWithVertexNeighbors.groupReduceOnNeighbors(new PropagateNeighborValues(), EdgeDirection.IN);

		DataSet<Vertex<String,TreeMap<String,Integer>>> verticesWithPropagatedTreeMaps = verticesWithPropagatedValues.groupBy(0).reduceGroup(
				new AttachNeighborIdsAsVertexValues());

		DataSet<Tuple1<Integer>> numberOfTriangles = verticesWithPropagatedTreeMaps
				.join(graph.getEdges())
				.where(0).equalTo(0).with(new CountTriangles()).reduce(new ReduceFunction<Tuple1<Integer>>() {

					@Override
					public Tuple1<Integer> reduce(Tuple1<Integer> firstTuple, Tuple1<Integer> secondTuple) throws Exception {
						return new Tuple1<Integer>(firstTuple.f0 + secondTuple.f0);
					}
				});

		// emit result
		if(fileOutput) {
			numberOfTriangles.writeAsCsv(outputPath, "\n", ",");
			env.execute("Executing Triangle Count");
		} else {
			numberOfTriangles.print();
		}
	}

	@Override
	public String getDescription() {
		return "Triangle Count Example";
	}

	@SuppressWarnings("serial")
	private static final class GatherHigherIdNeighbors implements
			NeighborsFunctionWithVertexValue<String, NullValue, NullValue, Vertex<String, String>> {

		@Override
		public void iterateNeighbors(Vertex<String, NullValue> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, NullValue>>> neighbors,
									 Collector<Vertex<String, String>> collector) throws Exception {

			Tuple2<Edge<String, NullValue>, Vertex<String, NullValue>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, NullValue>>> neighborsIterator =
					neighbors.iterator();
			long neighborCount = 0;
			long start = System.currentTimeMillis();
			String vertexKey = null;

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getId()) > Long.parseLong(vertex.getId())) {
					neighborCount++;
					vertexKey = next.f1.getId();

					collector.collect(new Vertex<String, String>(vertexKey, vertex.getId()));
				}
			}

			System.out.println("Gather " + neighborCount);

			long stop = System.currentTimeMillis();
			long time = stop - start;
			System.out.println("GatherTime " + time);
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachNeighborIdsAsVertexValues implements GroupReduceFunction<Vertex<String, String>,
			Vertex<String, TreeMap<String, Integer>>> {

		@Override
		public void reduce(Iterable<Vertex<String, String>> vertices,
						   Collector<Vertex<String, TreeMap<String, Integer>>> collector) throws Exception {
			Iterator<Vertex<String, String>> vertexIertator = vertices.iterator();
			Vertex<String, String> next = null;
			TreeMap<String,Integer> neighbors = new TreeMap<String,Integer>();
			String id = null;

			while (vertexIertator.hasNext()) {
				next = vertexIertator.next();
				id = next.getId();

				Integer value = neighbors.get(next.getValue());
				if (value != null) {
					neighbors.put(next.getValue(), value + 1);
				} else {
					neighbors.put(next.getValue(), 1);
				}
			}

			collector.collect(new Vertex<String, TreeMap<String, Integer>>(id, neighbors));
		}
	}

	@SuppressWarnings("serial")
	private static final class PropagateNeighborValues implements
			NeighborsFunctionWithVertexValue<String, TreeMap<String, Integer>, NullValue, Vertex<String, String>> {

		@Override
		public void iterateNeighbors(Vertex<String, TreeMap<String, Integer>> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, TreeMap<String, Integer>>>> neighbors,
									 Collector<Vertex<String, String>> collector) throws Exception {

			Tuple2<Edge<String, NullValue>, Vertex<String, TreeMap<String, Integer>>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, TreeMap<String, Integer>>>> neighborsIterator = neighbors.iterator();
			TreeMap<String, Integer> vertexSet = vertex.getValue();
			long neighborCount = 0;
			long start = System.currentTimeMillis();
			String vertexKey = null;

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getId()) > Long.parseLong(vertex.getId())) {
					for(String key: vertexSet.keySet()) {
						neighborCount ++;
						vertexKey = next.f1.getId();

						collector.collect(new Vertex<String, String>(vertexKey, key));
					}
				}
			}

			System.out.println("Compute " + neighborCount);

			long stop = System.currentTimeMillis();
			long time = stop -start;
			System.out.println("ComputeTime " + time);
		}
	}

	@SuppressWarnings("serial")
	private static final class CountTriangles implements
			FlatJoinFunction<Vertex<String, TreeMap<String, Integer>>, Edge<String, NullValue>, Tuple1<Integer>> {

		@Override
		public void join(Vertex<String, TreeMap<String, Integer>> vertex,
						 Edge<String, NullValue> edge, Collector<Tuple1<Integer>> collector) throws Exception {

			if (vertex.getValue().get(edge.getTarget()) != null) {
				collector.collect(new Tuple1<Integer>(vertex.getValue().get(edge.getTarget())));
			}
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
				System.err.println("Usage TriangleCount <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];

		} else {
			System.out.println("Executing TriangleCount example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage TriangleCount <edge path> <output path>");
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
