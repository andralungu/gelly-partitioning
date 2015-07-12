package example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
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
import util.DummyGraph;
import util.NodeSplittingData;
import util.TriangleCountData;

import java.io.File;
import java.util.Iterator;
import java.util.TreeMap;

public class NodeSplittingTriangleCount implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, NullValue, NullValue> graph = Graph.fromDataSet(edges, env).getUndirected();

		long numVertices = graph.numberOfVertices();

		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		double avg = getAverageDegree(numVertices, verticesWithDegrees);

		final double xMin = threshold;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, NullValue>, NullValue> graphWithSplitVertices =
				SplitVertex.treeDeAggregate(skewedVertices, graph, alpha, level, xMin);

		DummyGraph<String, Tuple2<String, NullValue>, NullValue> dummyGraphWithSplitVertices =
				DummyGraph.fromDataSet(graphWithSplitVertices.getVertices(),
						graphWithSplitVertices.getEdges(), env);

		// simulate the first superstep
		// select the neighbors with id greater than the current vertex's id
		DataSet<Vertex<String, Tuple2<String, String>>> verticesWithHigherNeighbors =
				dummyGraphWithSplitVertices.groupReduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> verticesWithNeighborHashSets =
				verticesWithHigherNeighbors.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		// you want to assign a value to the vertices with no neighbors as well
		DummyGraph<String, Tuple2<String, TreeMap<String, Integer>>, NullValue> graphWithInitializedVertexNeighbors =
				dummyGraphWithSplitVertices
						.mapVertices(new MapFunction<Vertex<String, Tuple2<String, NullValue>>,
								Tuple2<String, TreeMap<String, Integer>>>() {

							@Override
							public Tuple2<String, TreeMap<String, Integer>> map(Vertex<String, Tuple2<String, NullValue>> vertex) throws Exception {
								return new Tuple2<String, TreeMap<String, Integer>>(vertex.getValue().f0, new TreeMap<String, Integer>());
							}
						});

		DummyGraph<String, Tuple2<String, TreeMap<String, Integer>>, NullValue> dummyGraphWithVertexNeighbors = graphWithInitializedVertexNeighbors
				.joinWithVertices(verticesWithNeighborHashSets, new MapFunction<Vertex<Tuple2<String, TreeMap<String, Integer>>,
						Tuple2<String, TreeMap<String, Integer>>>, Tuple2<String, TreeMap<String, Integer>>>() {
					@Override
					public Tuple2<String, TreeMap<String, Integer>> map(Vertex<Tuple2<String, TreeMap<String, Integer>>,
							Tuple2<String, TreeMap<String, Integer>>> vertex) throws Exception {
						return vertex.getValue();
					}
				});

		Graph<String, Tuple2<String, TreeMap<String, Integer>>, NullValue> graphWithVertexNeighbors =
				Graph.fromDataSet(dummyGraphWithVertexNeighbors.getVertices(), graphWithSplitVertices.getEdges(), env);

		DataSet<Vertex<String, TreeMap<String, Integer>>> aggregatedVertices =
				SplitVertex.treeAggregate(verticesWithNeighborHashSets, level, new Aggregate())
				.map(new MapFunction<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>, Vertex<String, TreeMap<String, Integer>>>() {
					@Override
					public Vertex<String, TreeMap<String, Integer>> map(Vertex<String, Tuple2<String, TreeMap<String, Integer>>> vertex) throws Exception {
						return new Vertex<String, TreeMap<String, Integer>>(vertex.getId(), vertex.getValue().f1);
					}
				});

		// propagate the aggregated values to split vertices
		DataSet<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> updatedSplitVertices =
				SplitVertex.propagateValuesToSplitVertices(graphWithVertexNeighbors.getVertices(),
						aggregatedVertices);

		Graph<String, Tuple2<String, TreeMap<String, Integer>>, NullValue> graphAfterFirstSuperstep =
				Graph.fromDataSet(updatedSplitVertices, graphWithVertexNeighbors.getEdges(), env);

		DummyGraph<String, Tuple2<String, TreeMap<String, Integer>>, NullValue> dummyGraphAfterFirstSuperStep =
				DummyGraph.fromDataSet(graphAfterFirstSuperstep.getVertices(), graphAfterFirstSuperstep.getEdges(), env);

		// simulate the second superstep
		// propagate each received "message" to neighbours with higher id
		DataSet<Vertex<String, Tuple2<String, String>>> verticesWithPropagatedValues =
				dummyGraphAfterFirstSuperStep.groupReduceOnNeighbors(new PropagateNeighborValues(), EdgeDirection.IN);

		DataSet<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> verticesWithPropagatedTreeMaps = verticesWithPropagatedValues
				.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		DataSet<Vertex<String, TreeMap<String, Integer>>> aggregatedVerticesWithPropagatedTreeMaps =
				SplitVertex.treeAggregate(verticesWithPropagatedTreeMaps, level, new Aggregate())
						.map(new MapFunction<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>, Vertex<String, TreeMap<String, Integer>>>() {
							@Override
							public Vertex<String, TreeMap<String, Integer>> map(Vertex<String, Tuple2<String, TreeMap<String, Integer>>> vertex) throws Exception {
								return new Vertex<String, TreeMap<String, Integer>>(vertex.getId(), vertex.getValue().f1);
							}
						});

		// vertices are split; use the tags as keys instead
		DataSet<Tuple1<Integer>> numberOfTriangles = aggregatedVerticesWithPropagatedTreeMaps
				.join(graph.getEdges())
				.where(0).equalTo(0).with(new CountTriangles())
				.reduce(new ReduceFunction<Tuple1<Integer>>() {

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
			long neighborCount = 0;
			long start = System.currentTimeMillis();
			String vertexKey = null;

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getValue().f0) > Long.parseLong(vertex.getValue().f0)) {
					neighborCount ++;
					vertexKey = next.f1.getId();

					collector.collect(new Vertex<String, Tuple2<String, String>>(vertexKey,
							new Tuple2<String, String>(next.f1.getValue().f0, vertex.getValue().f0)));
				}
			}

			System.out.println("GatherSplit " + neighborCount);

			long stop = System.currentTimeMillis();
			long time = stop - start;
			System.out.println("GatherTimeSplit " + time);
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachNeighborIdsAsVertexValues implements GroupReduceFunction<Vertex<String, Tuple2<String, String>>,
			Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> {

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, String>>> vertices,
						   Collector<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, String>>> vertexIertator = vertices.iterator();
			Vertex<String, Tuple2<String, String>> next = null;
			TreeMap<String, Integer> neighbors = new TreeMap<String, Integer>();
			String id = null;
			String tag = null;

			while (vertexIertator.hasNext()) {
				next = vertexIertator.next();
				id = next.getId();
				tag = next.getValue().f0;

				// neighbor key already exists
				Integer value = neighbors.get(next.getValue().f1);
				if(value != null) {
					neighbors.put(next.getValue().f1, value + 1);
				} else {
					neighbors.put(next.getValue().f1, 1);
				}
			}

			collector.collect(new Vertex<String, Tuple2<String, TreeMap<String, Integer>>>(id,
					new Tuple2<String, TreeMap<String, Integer>>(tag, neighbors)));
		}
	}

	@SuppressWarnings("serial")
	private static final class Aggregate implements
			GroupReduceFunction<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>, Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> {

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> vertex,
						   Collector<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, TreeMap<String, Integer>>> next = null;
			TreeMap<String, Integer> result = new TreeMap<String, Integer>();
			String id = null;
			String tag = null;

			while (vertexIterator.hasNext()) {
				next = vertexIterator.next();
				id = next.getId();
				tag = next.getValue().f0;
				TreeMap<String, Integer> current = next.getValue().f1;
				for (String key : current.keySet()) {
					// neighbor key already exists
					Integer value = result.get(key);
					if (value != null) {
						result.put(key, value + current.get(key));
					} else {
						result.put(key, current.get(key));
					}
				}
			}

			collector.collect(new Vertex<String, Tuple2<String, TreeMap<String, Integer>>>(id,
					new Tuple2<String, TreeMap<String, Integer>>(tag, result)));
		}
	}

	@SuppressWarnings("serial")
	private static final class PropagateNeighborValues implements
			NeighborsFunctionWithVertexValue<String, Tuple2<String, TreeMap<String, Integer>>, NullValue,
					Vertex<String, Tuple2<String, String>>> {

		@Override
		public void iterateNeighbors(Vertex<String, Tuple2<String, TreeMap<String, Integer>>> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, TreeMap<String, Integer>>>>> neighbors,
									 Collector<Vertex<String, Tuple2<String, String>>> collector) throws Exception {

			Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, TreeMap<String, Integer>>>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, TreeMap<String, Integer>>>>> neighborsIterator = neighbors.iterator();
			TreeMap<String, Integer> vertexSet = vertex.getValue().f1;
			long neighborCount = 0;
			long start = System.currentTimeMillis();
			String vertexKey = null;

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(Long.parseLong(next.f1.getValue().f0) > Long.parseLong(vertex.getValue().f0)) {
					for(String key: vertexSet.keySet()) {
						neighborCount++;
						vertexKey = next.f1.getId();

						collector.collect(new Vertex<String, Tuple2<String, String>>(vertexKey,
								new Tuple2<String, String>(next.f1.getValue().f0, key)));
					}
				}
			}

			System.out.println("ComputeSplit " + neighborCount);

			long stop = System.currentTimeMillis();
			long time = stop - start;
			System.out.println("ComputeTimeSplit " + time);
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
					.fieldDelimiter(" ")
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
