package example;

import library.Jaccard;
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
import util.JaccardSimilarityMeasureData;
import util.NodeSplittingData;

import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

public class NodeSplittingJaccard implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, HashSet<String>, NullValue> initialGraph = Graph.fromDataSet(edges, new MapFunction<String, HashSet<String>>() {

			@Override
			public HashSet<String> map(String s) throws Exception {
				HashSet<String> neighborsHashSet = new HashSet<String>();
				neighborsHashSet.add(s);

				return neighborsHashSet;
			}
		}, env);

		DataSet<Tuple2<String, Long>> verticesWithDegrees = initialGraph.getDegrees();

		final double xMin = threshold;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithSplitVertices = SplitVertex.treeDeAggregate(skewedVertices, initialGraph,
				alpha, level, xMin);

		// compute neighbors
		// the result is stored within the vertex value
		DataSet<Vertex<String, HashSet<String>>> verticesWithNeighbors =
				Jaccard.getVerticesWithNeighborsForSplitVertices(graphWithSplitVertices);

		DataSet<Vertex<String, HashSet<String>>> aggregatedVertices =
				SplitVertex.treeAggregate(verticesWithNeighbors, level, new AggregateNeighborSets());

		// propagate computed aggregated values to the split vertices
		// avoid a second treeDeAggregate
		DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> updatedSplitVertices =
				SplitVertex.propagateValuesToSplitVertices(graphWithSplitVertices.getVertices(),
						aggregatedVertices);

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithNeighborsSplitVertices =
				Graph.fromDataSet(updatedSplitVertices, graphWithSplitVertices.getEdges(), env);

		DataSet<Vertex<String, TreeMap<String, Double>>> verticesWithJaccardValues = Jaccard
				.getVerticesWithJaccardValuesForSplitNodes(graphWithNeighborsSplitVertices);

		// introduce the custom "combiner" used for aggregation
		DataSet<Vertex<String, TreeMap<String, Double>>> aggregatedVerticesWithJaccardValues =
				SplitVertex.treeAggregate(verticesWithJaccardValues, level, new AggregateJaccardSets());

		// emit result
		if (fileOutput) {
			aggregatedVerticesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");
			env.execute("Node Splitting Jaccard Similarity Measure");
		} else {
			aggregatedVerticesWithJaccardValues.print();
		}
	}

	@Override
	public String getDescription() {
		return "Jaccard with Split Vertices";
	}

	/**
	 * UDF that describes the combining strategy: the partial hash sets containing neighbor values will be reduced
	 * to a single hash set per vertex.
	 */
	@SuppressWarnings("serial")
	private static final class AggregateNeighborSets implements GroupReduceFunction<Vertex<String, HashSet<String>>, Vertex<String, HashSet<String>>> {

		@Override
		public void reduce(Iterable<Vertex<String, HashSet<String>>> vertex,
						   Collector<Vertex<String, HashSet<String>>> collector) throws Exception {

			Iterator<Vertex<String, HashSet<String>>> vertexIterator = vertex.iterator();
			Vertex<String, HashSet<String>> next = null;
			HashSet<String> neighborsPerVertex = new HashSet<>();
			String id = null;

			while (vertexIterator.hasNext()) {
				next = vertexIterator.next();
				id = next.getId();
				neighborsPerVertex.addAll(next.getValue());
			}
			collector.collect(new Vertex<String, HashSet<String>>(id, neighborsPerVertex));
		}
	}

	/**
	 * UDF that describes the combining strategy: the partial tree maps with Jaccard values will be reduced to a single
	 * tree map per vertex.
	 */
	@SuppressWarnings("serial")
	private static final class AggregateJaccardSets implements GroupReduceFunction<Vertex<String, TreeMap<String, Double>>,
			Vertex<String, TreeMap<String, Double>>> {

		@Override
		public void reduce(Iterable<Vertex<String, TreeMap<String, Double>>> vertex,
						   Collector<Vertex<String, TreeMap<String, Double>>> collector) throws Exception {

			Iterator<Vertex<String, TreeMap<String, Double>>> vertexIterator = vertex.iterator();
			Vertex<String, TreeMap<String, Double>> next = null;
			TreeMap<String, Double> result = new TreeMap<String, Double>();
			String id = null;

			while (vertexIterator.hasNext()) {
				next = vertexIterator.next();
				id = next.getId();
				result.putAll(next.getValue());
			}

			collector.collect(new Vertex<String, TreeMap<String, Double>>(id, result));
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
				System.err.println("Usage NodeSplittingJaccard <edge path> <output path> <alpha> <level> <threshold>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			alpha = Integer.parseInt(args[2]);
			level = Integer.parseInt(args[3]);
			threshold = Integer.parseInt(args[4]);
		} else {
			System.out.println("Executing NodeSplittingJaccard example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage NodeSplittingJaccard <edge path> <output path> <alpha> <level> <threshold>");
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
