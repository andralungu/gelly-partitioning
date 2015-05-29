package example;

import library.Jaccard;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import util.JaccardSimilarityMeasureData;

import java.util.HashSet;
import java.util.TreeMap;

public class JaccardSimilarityMeasure implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, HashSet<String>, NullValue> graph = Graph.fromDataSet(edges,
				new MapFunction<String, HashSet<String>>() {
					@Override
					public HashSet<String> map(String s) throws Exception {
						HashSet<String> neighborsHashSet = new HashSet<String>();
						neighborsHashSet.add(s);

						return neighborsHashSet;
					}
				}, env);

		// the result is stored within the vertex value
		DataSet<Vertex<String, HashSet<String>>> verticesWithNeighbors =
				Jaccard.getVerticesWithNeighbors(graph);

		Graph<String, HashSet<String>, NullValue> undirectedGraphWithVertexValues =
				Graph.fromDataSet(verticesWithNeighbors, edges, env).getUndirected();

		// simulate a vertex centric iteration with groupReduceOnNeighbors
		DataSet<Vertex<String, TreeMap<String, Double>>> verticesWithJaccardValues =
				Jaccard.getVerticesWithJaccardValues(undirectedGraphWithVertexValues);

		// emit result
		if (fileOutput) {
			verticesWithJaccardValues.writeAsCsv(outputPath, "\n", ",");
			env.execute("Executing Jaccard Similarity Measure");
		} else {
			verticesWithJaccardValues.print();
		}
	}


	@Override
	public String getDescription() {
		return "Vertex Jaccard Similarity Measure";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage JaccardSimilarityMeasure <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing JaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage JaccardSimilarityMeasure <edge path> <output path>");
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
