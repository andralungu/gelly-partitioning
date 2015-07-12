package example;

import library.CommunityDetection;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import util.CommunityDetectionData;

public class CommunityDetectionExample implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// set up the graph
		DataSet<Edge<String, Double>> edges = getEdgesDataSet(env);
		Graph<String, Long, Double> graph = Graph.fromDataSet(edges,
				new StringToLongMap(), env);

		// the result is in the form of <vertexId, communityId>, where the communityId is the label
		// which the vertex converged to
		DataSet<Vertex<String, Long>> communityVertices =
				graph.run(new CommunityDetection(maxIterations, delta)).getVertices();

		// emit result
		if (fileOutput) {
			communityVertices.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Executing Community Detection Example");
		} else {
			communityVertices.print();
		}
	}

	@Override
	public String getDescription() {
		return "Community Detection";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = CommunityDetectionData.MAX_ITERATIONS;
	private static Double delta = CommunityDetectionData.DELTA;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage CommunityDetection <edge path> <output path> " +
						"<num iterations> <delta>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			delta = Double.parseDouble(args[3]);
		} else {
			System.out.println("Executing SimpleCommunityDetection example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage CommunityDetection <edge path> <output path> " +
					"<num iterations> <delta>");
		}
		return true;
	}

	private static DataSet<Edge<String, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if(fileOutput) {

			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(String.class, String.class, Double.class)
					.map(new Tuple3ToEdgeMap<String, Double>());
		} else {
			return CommunityDetectionData.getDefaultEdgeDataSet(env);
		}
	}

	@SuppressWarnings("serial")
	private static final class StringToLongMap implements MapFunction<String, Long> {

		@Override
		public Long map(String s) throws Exception {
			return Long.parseLong(s);
		}
	}
}