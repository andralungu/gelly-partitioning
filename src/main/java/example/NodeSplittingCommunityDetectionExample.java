package example;

import library.NodeSplittingCommunityDetection;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.NullValue;
import splitUtils.SplitVertex;
import util.CommunityDetectionData;
import util.NodeSplittingData;

public class NodeSplittingCommunityDetectionExample implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, Double>> edges = getEdgesDataSet(env);
		Graph<String, Long, Double> initialGraph = Graph.fromDataSet(edges,
				new StringToLongMap(), env);

		// determine skewed nodes using a threshold
		long numVertices = initialGraph.numberOfVertices();
		DataSet<Tuple2<String, Long>> verticesWithDegrees = initialGraph.getDegrees();
		double avg = getAverageDegree(numVertices, verticesWithDegrees);
		double xMin = threshold * avg;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, Long>, Double> graphWithSplitVertices =
				SplitVertex.treeDeAggregate(skewedVertices, initialGraph, alpha, level, xMin);

		// run the algorithm on the new graph
		DataSet<Vertex<String, Long>> communityVertices =
				graphWithSplitVertices.run(new NodeSplittingCommunityDetection(maxIterations, delta, level))
						.getVertices().map(new MapFunction<Vertex<String, Tuple2<String, Long>>, Vertex<String, Long>>() {
					@Override
					public Vertex<String, Long> map(Vertex<String, Tuple2<String, Long>> vertex) throws Exception {
						return new Vertex<String, Long>(vertex.getId(), vertex.getValue().f1);
					}
				});

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
		return "Node Splitting Community Detection";
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
	private static Integer maxIterations = CommunityDetectionData.MAX_ITERATIONS;
	private static Double delta = CommunityDetectionData.DELTA;
	private static Integer threshold = CommunityDetectionData.THRESHOLD;
	private static Integer alpha = NodeSplittingData.ALPHA;
	private static Integer level = NodeSplittingData.LEVEL;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 7) {
				System.err.println("Usage CommunityDetection <edge path> <output path> " +
						"<num iterations> <delta> <split threshold> <alpha> <level>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			delta = Double.parseDouble(args[3]);
			threshold = Integer.parseInt(args[4]);
			alpha = Integer.parseInt(args[5]);
			level = Integer.parseInt(args[6]);
		} else {
			System.out.println("Executing SimpleCommunityDetection example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage CommunityDetection <edge path> <output path> " +
					"<num iterations> <delta> <split threshold> <alpha> <level>");
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
