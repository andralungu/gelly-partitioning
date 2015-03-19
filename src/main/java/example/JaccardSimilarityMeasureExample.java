package example;

import example.util.JaccardSimilarityMeasureData;
import library.JaccardSimilarityMeasure;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class JaccardSimilarityMeasureExample implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		// the result is stored within the vertex value
		DataSet<Vertex<Long, Tuple2<HashSet<Long>,HashMap<Long, Double>>>> verticesWithNeighbors =
				graph.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);

		Graph<Long, Tuple2<HashSet<Long>,HashMap<Long, Double>>, NullValue> graphWithVertexValues =
				Graph.fromDataSet(verticesWithNeighbors, edges, env);

		// set up the program
		Graph<Long, Tuple2<HashSet<Long>,HashMap<Long, Double>>, NullValue> jaccardGraph =
				graphWithVertexValues.run(new JaccardSimilarityMeasure(maxIterations));

		// get the result (only the Jaccard coefficients)
		DataSet<Tuple2<Long, HashMap<Long, Double>>> resultedVertices = jaccardGraph.getVertices()
				.map(new MapFunction<Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>, Tuple2<Long, HashMap<Long, Double>>>() {

					@Override
					public Tuple2<Long, HashMap<Long, Double>> map(Vertex<Long, Tuple2<HashSet<Long>,
							HashMap<Long, Double>>> vertex) throws Exception {
						return new Tuple2<Long, HashMap<Long, Double>>(vertex.getId(), vertex.getValue().f1);
					}
				});

		// emit result
		if (fileOutput) {
			resultedVertices.writeAsCsv(outputPath, "\n", ",");
		} else {
			resultedVertices.print();
		}

		env.execute("Executing Jaccard Similarity Measure");
	}

	@Override
	public String getDescription() {
		return "Vertex Jaccard Similarity Measure";
	}

	/**
	 * Each vertex will have a HashSet containing its neighbors as value.
	 */
	@SuppressWarnings("serial")
	private static final class GatherNeighbors implements NeighborsFunction<Long, NullValue, NullValue,
			Vertex<Long, Tuple2<HashSet<Long>,HashMap<Long, Double>>>> {

		@Override
		public Vertex<Long, Tuple2<HashSet<Long>,HashMap<Long, Double>>> iterateNeighbors(Iterable<Tuple3<Long, Edge<Long, NullValue>,
				Vertex<Long, NullValue>>> neighbors) throws Exception {

			HashSet<Long> neighborsHashSet = new HashSet<Long>();
			Tuple3<Long, Edge<Long, NullValue>, Vertex<Long, NullValue>> next = null;
			Iterator<Tuple3<Long, Edge<Long, NullValue>, Vertex<Long, NullValue>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				neighborsHashSet.add(next.f2.getId());
			}

			return new Vertex<Long, Tuple2<HashSet<Long>, HashMap<Long, Double>>>(next.f0,
					new Tuple2<>(neighborsHashSet, new HashMap<Long, Double>()));
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = JaccardSimilarityMeasureData.MAX_ITERATIONS;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage JaccardSimilarityMeasureExample <edge path> <output path> <maxIterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing JaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage JaccardSimilarityMeasureExample <edge path> <output path> <maxIterations>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<Long, NullValue>(tuple2.f0, tuple2.f1, NullValue.getInstance());
						}
					});
		} else {
			return JaccardSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}
