package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import splitUtils.SplitVertex;
import util.NodeSplittingData;
import util.TriangleCountData;

import java.util.HashSet;
import java.util.Iterator;

public class NodeSplittingTriangleCount implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		long numVertices = graph.numberOfVertices();

		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		double avg = getAverageDegree(numVertices, verticesWithDegrees);

		final double xMin = threshold * avg;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, NullValue>, NullValue> graphWithSplitVertices =
				SplitVertex.treeDeAggregate(skewedVertices, graph, alpha, level, xMin);

		// simulate the first superstep
		// select the neighbors with id greater than the current vertex's id
		DataSet<Vertex<String, Tuple2<String, String>>> verticesWithHigherNeighbors =
				graphWithSplitVertices.groupReduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.ALL);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> verticesWithNeighborHashSets =
				verticesWithHigherNeighbors.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithVertexNeighbors = Graph.fromDataSet(verticesWithNeighborHashSets,
				graphWithSplitVertices.getEdges(), env);

		DataSet<Vertex<String, HashSet<String>>> aggregatedVertices =
				SplitVertex.treeAggregate(verticesWithNeighborHashSets, level, new Aggregate())
				.map(new MapFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Vertex<String, HashSet<String>>>() {
					@Override
					public Vertex<String, HashSet<String>> map(Vertex<String, Tuple2<String, HashSet<String>>> vertex) throws Exception {
						return new Vertex<String, HashSet<String>>(vertex.getId(), vertex.getValue().f1);
					}
				});

		// propagate the aggregated values to split vertices
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> updatedSplitVertices =
				SplitVertex.propagateValuesToSplitVertices(graphWithVertexNeighbors.getVertices(),
						aggregatedVertices);

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphAfterFirstSuperstep =
				Graph.fromDataSet(updatedSplitVertices, graphWithVertexNeighbors.getEdges(), env);

		// simulate the second superstep
		// propagate each received "message" to neighbours with higher id
		DataSet<Vertex<String, Tuple2<String, String>>> verticesWithPropagatedValues =
				graphAfterFirstSuperstep.groupReduceOnNeighbors(new PropagateNeighborValues(), EdgeDirection.ALL);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> verticesWithPropagatedHashSets =
				verticesWithPropagatedValues.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		// aggregate vertices
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> aggregatedVerticesSuperstepTwo =
				SplitVertex.treeAggregate(verticesWithPropagatedHashSets, level,
						new Aggregate());

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithPropagatedVertexValues =
				Graph.fromDataSet(aggregatedVerticesSuperstepTwo,
				edges, env).getUndirected();

		DataSet<Vertex<String, Integer>> verticesWithNumberOfTriangles = graphWithPropagatedVertexValues.getVertices()
				.map(new MapFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Vertex<String, HashSet<String>>>() {
					@Override
					public Vertex<String, HashSet<String>> map(Vertex<String, Tuple2<String, HashSet<String>>> vertex) throws Exception {
						return new Vertex<String, HashSet<String>>(vertex.getId(), vertex.getValue().f1);
					}
				})
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
		return "Node Splitting Triangle Count";
	}

	@SuppressWarnings("serial")
	private static final class GatherHigherIdNeighbors implements
			NeighborsFunctionWithVertexValue<String, Tuple2<String, NullValue>, NullValue, Vertex<String, Tuple2<String, String>>> {

		@Override
		public void iterateNeighbors(Vertex<String, Tuple2<String, NullValue>> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, NullValue>>>> neighbors,
									 Collector<Vertex<String, Tuple2<String, String>>> collector) throws Exception {

			Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, NullValue>>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, NullValue>>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getValue().f0) > Long.parseLong(vertex.getValue().f0)) {
					collector.collect(new Vertex<String, Tuple2<String, String>>(next.f1.getId(),
							new Tuple2<String, String>(next.f1.getValue().f0, vertex.getValue().f0)));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachNeighborIdsAsVertexValues implements GroupReduceFunction<Vertex<String, Tuple2<String, String>>,
			Vertex<String, Tuple2<String, HashSet<String>>>> {

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, String>>> vertices,
						   Collector<Vertex<String, Tuple2<String, HashSet<String>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, String>>> vertexIertator = vertices.iterator();
			Vertex<String, Tuple2<String, String>> next = null;
			HashSet<String> neighbors = new HashSet<String>();
			String id = null;
			String tag = null;

			while (vertexIertator.hasNext()) {
				next = vertexIertator.next();
				id = next.getId();
				tag = next.getValue().f0;
				neighbors.add(next.getValue().f1);
			}

			collector.collect(new Vertex<String, Tuple2<String, HashSet<String>>>(id,
					new Tuple2<String, HashSet<String>>(tag, neighbors)));
		}
	}

	@SuppressWarnings("serial")
	private static final class Aggregate implements
			GroupReduceFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Vertex<String, Tuple2<String, HashSet<String>>>> {

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, HashSet<String>>>> vertex,
						   Collector<Vertex<String, Tuple2<String, HashSet<String>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, HashSet<String>>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, HashSet<String>>> next = null;
			HashSet<String> result = new HashSet<String>();
			String id = null;
			String tag = null;

			while (vertexIterator.hasNext()) {
				next = vertexIterator.next();
				id = next.getId();
				tag = next.getValue().f0;
				result.addAll(next.getValue().f1);
			}

			collector.collect(new Vertex<String, Tuple2<String, HashSet<String>>>(id,
					new Tuple2<String, HashSet<String>>(tag, result)));
		}
	}

	@SuppressWarnings("serial")
	private static final class PropagateNeighborValues implements
			NeighborsFunctionWithVertexValue<String, Tuple2<String, HashSet<String>>, NullValue,
					Vertex<String, Tuple2<String, String>>> {

		@Override
		public void iterateNeighbors(Vertex<String, Tuple2<String, HashSet<String>>> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>>> neighbors,
									 Collector<Vertex<String, Tuple2<String, String>>> collector) throws Exception {

			Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>>> neighborsIterator = neighbors.iterator();
			HashSet<String> vertexSet = vertex.getValue().f1;

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getValue().f0) > Long.parseLong(vertex.getValue().f0)) {
					for(String key: vertexSet) {
						collector.collect(new Vertex<String, Tuple2<String, String>>(next.f1.getId(),
								new Tuple2<String, String>(next.f1.getValue().f0, key)));
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

	/**
	 * Helper method that computes the average degree given a DataSet of degrees,
	 * and the number of vertices.
	 *
	 * @param numVertices
	 * @param verticesWithDegrees
	 * @return the average degree
	 */
	private static double getAverageDegree(long numVertices, DataSet<Tuple2<String, Long>> verticesWithDegrees) throws Exception {

		DataSet<Double> avgNodeDegree = verticesWithDegrees
				.aggregate(Aggregations.SUM, 1).map(new AvgNodeDegreeMapper(numVertices));

		return avgNodeDegree.collect().get(0);
	}

	@SuppressWarnings("serial")
	private static final class AvgNodeDegreeMapper implements MapFunction<Tuple2<String, Long>, Double> {

		private long numberOfVertices;

		public AvgNodeDegreeMapper(long numberOfVertices) {
			this.numberOfVertices = numberOfVertices;
		}

		public Double map(Tuple2<String, Long> sumTuple) {
			return (double) (sumTuple.f1 / numberOfVertices) ;
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static Integer alpha = NodeSplittingData.ALPHA;
	private static Integer level = NodeSplittingData.LEVEL;
	private static Integer threshold = NodeSplittingData.THRESHOLD;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 5) {
				System.err.println("Usage TriangleCount <edge path> <output path> <alpha> <level> <threshold>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			alpha = Integer.parseInt(args[2]);
			level = Integer.parseInt(args[3]);
			threshold = Integer.parseInt(args[4]);

		} else {
			System.out.println("Executing TriangleCount example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage TriangleCount <edge path> <output path> <alpha> <level> <threshold>");
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
