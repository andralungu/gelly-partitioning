package partitioning;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import util.NodeSplittingData;

public class PartitioningExample {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);
		Graph<String, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		// discover the skewed nodes
		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(threshold,
				verticesWithDegrees);

		DataSet<Vertex<String, NullValue>> unskewedVertices = determineUnskewedVertices(threshold,
				verticesWithDegrees);

		DataSet<Edge<String, NullValue>> joinedEdges;

		// compute a data set of joinedEdges for skewed vertices, with vertex-cut partitioning
		DataSet<Edge<String, NullValue>> joinedEdgesSkewed = edges.join(skewedVertices)
				.where(0).equalTo(0).with(new CollectEdge()).partitionCustom(new SourcePartition(), 1);

		// and a data set of joinedEdges for regular vertices, with edge-cut partitioning
		DataSet<Edge<String, NullValue>> joinedEdgesUnskewed = edges.join(unskewedVertices)
				.where(0).equalTo(0).with(new CollectEdge()).partitionCustom(new TargetPartition(), 0);

		// union the data sets to get the final result
		joinedEdges = joinedEdgesSkewed.union(joinedEdgesUnskewed);

		// emit result
		if(fileOutput) {
			joinedEdges.writeAsCsv(outputPath, "\n", ",");
			env.execute("Executing partitioning Example");
		} else {
			joinedEdges.print();
		}

	}

	public static DataSet<Vertex<String, NullValue>> determineUnskewedVertices(final double xMin,
																			 DataSet<Tuple2<String, Long>> verticesWithDegrees) throws Exception {
		return verticesWithDegrees
				.flatMap(new FlatMapFunction<Tuple2<String, Long>, Vertex<String, NullValue>>() {

					@Override
					public void flatMap(Tuple2<String, Long> vertexIdDegree,
										Collector<Vertex<String, NullValue>> collector) throws Exception {
						if(vertexIdDegree.f1 <= xMin) {
							collector.collect(new Vertex<String, NullValue>(vertexIdDegree.f0, NullValue.getInstance()));
						}
					}
				});
	}

	public static final class CollectEdge implements FlatJoinFunction<Edge<String, NullValue>,
			Vertex<String, NullValue>, Edge<String, NullValue>> {

		@Override
		public void join(Edge<String, NullValue> edge,
						 Vertex<String, NullValue> vertex,
						 Collector<Edge<String, NullValue>> collector) throws Exception {

			collector.collect(edge);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static Integer threshold = NodeSplittingData.THRESHOLD;

	private static boolean parseParameters(String[] args) {
		if (args.length > 0) {
			if (args.length != 3) {
				System.err.println("Usage PartitioningExample <edge path> <output path> <threshold>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			threshold = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing PartitioningExample with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage PartitioningExample <edge path> <output path> <threshold>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<String, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
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
			return NodeSplittingData.getDefaultEdgeDataSet(env);

		}
	}
}
