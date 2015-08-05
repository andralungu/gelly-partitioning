package example;

import library.ConnectedComponentsAlgorithm;
import org.apache.flink.api.common.ProgramDescription;
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
import util.ConnectedComponentsData;

public class ConnectedComponents implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, Long, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<String, Long>() {
			@Override
			public Long map(String s) throws Exception {
				return Long.parseLong(s);
			}
		}, env);

		DataSet<Vertex<String, Long>> verticesWithMinIds = graph
				.run(new ConnectedComponentsAlgorithm(maxIterations)).getVertices();

		// emit result
		if (fileOutput) {
			verticesWithMinIds.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Connected Components Example");
		} else {
			verticesWithMinIds.print();
		}


	}

	@Override
	public String getDescription() {
		return "Connected Components Example";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String vertexInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = ConnectedComponentsData.MAX_ITERATIONS;

	private static boolean parseParameters(String[] args) {
		if (args.length > 0) {
			if (args.length != 3) {
				System.err.println("Usage ConnectedComponents <edge path> <output path> " +
						"<num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);

		} else {
			System.out.println("Executing ConnectedComponents example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage ConnectedComponents <edge path> <output path> " +
					"<num iterations>");
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
						public Edge<String, NullValue> map(Tuple2<String, String> value) throws Exception {
							return new Edge<String, NullValue>(value.f0, value.f1, NullValue.getInstance());
						}
					});
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}
}
