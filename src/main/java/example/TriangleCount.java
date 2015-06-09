package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import util.TriangleCountData;

import java.util.HashSet;
import java.util.Iterator;

public class TriangleCount implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		// simulate the first superstep
		// select the neighbors with id greater than the current vertex's id
		DataSet<Vertex<String, String>> verticesWithHigherNeighbors =
				graph.groupReduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.ALL);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<String, HashSet<String>>> verticesWithNeighborHashSets =
				verticesWithHigherNeighbors.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		Graph<String, HashSet<String>, NullValue> graphWithVertexNeighbors = Graph.fromDataSet(verticesWithNeighborHashSets,
				edges, env);

		// simulate the second superstep
		// propagate each received "message" to neighbours with higher id
		DataSet<Vertex<String, String>> verticesWithPropagatedValues =
				graphWithVertexNeighbors.groupReduceOnNeighbors(new PropagateNeighborValues(), EdgeDirection.ALL);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<String, HashSet<String>>> verticesWithPropagatedHashSets =
				verticesWithPropagatedValues.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		Graph<String, HashSet<String>, NullValue> graphWithPropagatedVertexValues = Graph.fromDataSet(verticesWithPropagatedHashSets,
				edges, env).getUndirected();

		DataSet<Vertex<String, Integer>> verticesWithNumberOfTriangles = graphWithPropagatedVertexValues.getVertices()
				.join(graphWithPropagatedVertexValues.getEdges())
				.where(0).equalTo(0).with(new CountTriangles());

		DataSet<Tuple1<Integer>> numberOfTriangles =  verticesWithNumberOfTriangles
				.flatMap(new FlatMapFunction<Vertex<String, Integer>, Tuple1<Integer>>() {

					@Override
					public void flatMap(Vertex<String, Integer> vertex, Collector<Tuple1<Integer>> collector) throws Exception {
						collector.collect(new Tuple1<Integer>(vertex.getValue()));
					}
				}).reduce(new ReduceFunction<Tuple1<Integer>>() {

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

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getId()) > Long.parseLong(vertex.getId())) {
					collector.collect(new Vertex<String, String>(next.f1.getId(), vertex.getId()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachNeighborIdsAsVertexValues implements GroupReduceFunction<Vertex<String, String>,
			Vertex<String, HashSet<String>>> {

		@Override
		public void reduce(Iterable<Vertex<String, String>> vertices,
						   Collector<Vertex<String, HashSet<String>>> collector) throws Exception {
			Iterator<Vertex<String, String>> vertexIertator = vertices.iterator();
			Vertex<String, String> next = null;
			HashSet<String> neighbors = new HashSet<String>();
			String id = null;

			while (vertexIertator.hasNext()) {
				next = vertexIertator.next();
				id = next.getId();
				neighbors.add(next.getValue());
			}

			collector.collect(new Vertex<String, HashSet<String>>(id, neighbors));
		}
	}

	@SuppressWarnings("serial")
	private static final class PropagateNeighborValues implements
			NeighborsFunctionWithVertexValue<String, HashSet<String>, NullValue, Vertex<String, String>> {

		@Override
		public void iterateNeighbors(Vertex<String, HashSet<String>> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, HashSet<String>>>> neighbors,
									 Collector<Vertex<String, String>> collector) throws Exception {

			Tuple2<Edge<String, NullValue>, Vertex<String, HashSet<String>>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, HashSet<String>>>> neighborsIterator = neighbors.iterator();
			HashSet<String> vertexSet = vertex.getValue();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getId()) > Long.parseLong(vertex.getId())) {
					for(String key: vertexSet) {
						collector.collect(new Vertex<String, String>(next.f1.getId(), key));
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class CountTriangles implements
			JoinFunction<Vertex<String, HashSet<String>>, Edge<String, NullValue>, Vertex<String, Integer>> {

		@Override

		public Vertex<String, Integer> join(Vertex<String, HashSet<String>> vertex,
											Edge<String, NullValue> edge) throws Exception {
			int numberOfTriangles = 0;

			if(vertex.getValue().contains(edge.getTarget())) {
				numberOfTriangles++;
			}

			return new Vertex<String, Integer>(vertex.getId(), numberOfTriangles);
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
