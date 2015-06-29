package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.NullValue;
import splitUtils.SplitVertex;
import util.CommunityDetectionData;
import util.NodeSplittingData;

public class NodeSplittingCommunityDetection implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, Double>> edges = getEdgesDataSet(env);
		Graph<String, Long, Double> graph = Graph.fromDataSet(edges,
				new StringToLongMap(), env);

		// Step 1: Discover the skewed nodes and do the splitting (keeping the original vertex id)
		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		final double xMin = threshold;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, Long>, Double> graphWithSplitVertices =
				SplitVertex.treeDeAggregate(skewedVertices, graph, alpha, level, xMin);

		// Step 2: Create a delta iteration that takes the split vertex set as a solution set
		// At the end of each superstep, group by vertex id(tag), do the merging and update the vertex value.

	}

	@Override
	public String getDescription() {
		return "Node Splitting Community Detection";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = CommunityDetectionData.MAX_ITERATIONS;
	private static Double delta = CommunityDetectionData.DELTA;

	private static Integer alpha = NodeSplittingData.ALPHA;
	private static Integer level = NodeSplittingData.LEVEL;
	private static Integer threshold = NodeSplittingData.THRESHOLD;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 7) {
				System.err.println("Usage NodeSplittingCommunityDetection <edge path> <output path> " +
						"<num iterations> <delta> <alpha> <level> <threshold>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			delta = Double.parseDouble(args[3]);

			alpha = Integer.parseInt(args[4]);
			level = Integer.parseInt(args[5]);
			threshold = Integer.parseInt(args[6]);
		} else {
			System.out.println("Executing NodeSplittingCommunityDetection example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage CommunityDetection <edge path> <output path> " +
					"<num iterations> <delta> <alpha> <level> <threshold>");
		}
		return true;
	}

	private static DataSet<Edge<String, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if(fileOutput) {

			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
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
