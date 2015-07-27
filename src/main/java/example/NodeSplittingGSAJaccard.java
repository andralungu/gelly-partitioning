package example;

import library.GSAJaccard;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import splitUtils.SplitVertex;
import util.DummyGraph;
import util.JaccardSimilarityMeasureData;
import util.NodeSplittingData;

import java.util.HashSet;
import java.util.Iterator;

public class NodeSplittingGSAJaccard implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		// initialize the ermivertex values with hash sets containing their own ids
		Graph<String, HashSet<String>, NullValue> graph = Graph.fromDataSet(edges,
				new MapFunction<String, HashSet<String>>() {

					@Override
					public HashSet<String> map(String id) throws Exception {
						HashSet<String> neighbors = new HashSet<String>();
						neighbors.add(id);

						return new HashSet<String>(neighbors);
					}
				}, env);

		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		final double xMin = threshold;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithSplitVertices = SplitVertex
				.treeDeAggregate(skewedVertices, graph, alpha, level, xMin);

		// Simulate GSA
		// Gather: no-op in this case
		// Sum: create the set of neighbors
		DataSet<Tuple2<String, Tuple2<String, HashSet<String>>>> computedNeighbors =
				GSAJaccard.getVerticesWithNeighborsForSplitVertces(graphWithSplitVertices);

		// if the given subvertex has no value, joinWithVertices will keep the initial value,
		// yielding erroneous results
		graphWithSplitVertices = graphWithSplitVertices.mapVertices(new MapFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Tuple2<String, HashSet<String>>>() {
			@Override
			public Tuple2<String, HashSet<String>> map(Vertex<String, Tuple2<String, HashSet<String>>> vertex) throws Exception {
				// get the initial id and a clean hash set
				return new Tuple2<String, HashSet<String>>(vertex.getValue().f0, new HashSet<String>());
			}
		});

		// Apply: attach the computed values to the vertices
		// joinWithVertices to update the node values
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> verticesWithNeighbors =
				GSAJaccard.attachValuesToVerticesForSplitVertices(graphWithSplitVertices, computedNeighbors);

		DataSet<Vertex<String, HashSet<String>>> aggregatedVertices =
				SplitVertex.treeAggregate(verticesWithNeighbors, level, new AggregateNeighborSets())
				.map(new MapFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Vertex<String, HashSet<String>>>() {

					@Override
					public Vertex<String, HashSet<String>> map(Vertex<String, Tuple2<String, HashSet<String>>> vertex) throws Exception {
						return new Vertex<String, HashSet<String>>(vertex.getId(), vertex.getValue().f1);
					}
				});

		// propagate computed aggregated values to the split vertices
		// avoid a second treeDeAggregate
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> updatedSplitVertices =
				SplitVertex.propagateValuesToSplitVertices(graphWithSplitVertices.getVertices(),
						aggregatedVertices);

		DummyGraph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithNeighbors =
				DummyGraph.fromDataSet(updatedSplitVertices, graphWithSplitVertices.getEdges(), env);

		// Scatter: compare neighbors; compute Jaccard
		DataSet<Edge<String, Double>> splitEdgesWithJaccardValues =
				GSAJaccard.computeJaccardForSplitVertices(graphWithNeighbors);

		DataSet<Edge<String, Double>> edgesWithJaccardValues =
				SplitVertex.cleanupEdges(splitEdgesWithJaccardValues);

		// emit result
		if (fileOutput) {
			edgesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");
			env.execute("Node Splitting GSA Jaccard Similarity Measure");
		} else {
			edgesWithJaccardValues.print();
		}
	}

	@Override
	public String getDescription() {
		return "Node Splitting GSA Jaccard Similarity Measure";
	}

	/**
	 * UDF that describes the combining strategy: the partial hash sets containing neighbor values will be reduced
	 * to a single hash set per vertex.
	 */
	@SuppressWarnings("serial")
	private static final class AggregateNeighborSets implements
			GroupReduceFunction<Vertex<String, Tuple2<String, HashSet<String>>>, Vertex<String, Tuple2<String, HashSet<String>>>>{

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, HashSet<String>>>> vertex,
						   Collector<Vertex<String, Tuple2<String, HashSet<String>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, HashSet<String>>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, HashSet<String>>> next = null;
			HashSet<String> neighborsPerVertex = new HashSet<>();
			String id = null;

			while (vertexIterator.hasNext()) {
				next = vertexIterator.next();
				id = next.getId();
				neighborsPerVertex.addAll(next.getValue().f1);
			}

			collector.collect(new Vertex<String, Tuple2<String, HashSet<String>>>(id,
					new Tuple2<String, HashSet<String>>(id, neighborsPerVertex)));
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
				System.err.println("Usage JaccardSimilarityMeasure <edge path> <output path> <alpha> <level> <threshold>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			alpha = Integer.parseInt(args[2]);
			level = Integer.parseInt(args[3]);
			threshold = Integer.parseInt(args[4]);
		} else {
			System.out.println("Executing JaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage JaccardSimilarityMeasure <edge path> <output path> <alpha> <level> <threshold>");
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
