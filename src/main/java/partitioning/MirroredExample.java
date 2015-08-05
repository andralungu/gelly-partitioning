package partitioning;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import util.NodeSplittingData;

/**
 * A mirror of the partitioning example - same operations; no custom partitioning
 */
public class MirroredExample {

	public static void main (String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);
		Graph<String, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		DataSet<Edge<String, NullValue>> joinedEdges = edges.join(graph.getVertices())
				.where(0).equalTo(0).with(new FlatJoinFunction<Edge<String, NullValue>, Vertex<String, NullValue>, Edge<String, NullValue>>() {
					@Override
					public void join(Edge<String, NullValue> edge,
									 Vertex<String, NullValue> vertex,
									 Collector<Edge<String, NullValue>> collector) throws Exception {

						collector.collect(edge);
					}
				});

		// emit result
		if(fileOutput) {
			joinedEdges.writeAsCsv(outputPath, "\n", ",");
			env.execute("Executing mirrored Example");
		} else {
			joinedEdges.print();
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static boolean parseParameters(String[] args) {
		if (args.length > 0) {
			if (args.length != 2) {
				System.err.println("Usage MirroredExample <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing PartitioningExample with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage MirroredExample <edge path> <output path>");
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
