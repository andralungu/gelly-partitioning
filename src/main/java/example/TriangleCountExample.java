package example;

import example.util.TriangleCountData;
import library.TriangleCount;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;

public class TriangleCountExample implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// set up the graph
		DataSet<Edge<Integer, NullValue>> edges = getEdgesDataSet(env);

		Graph<Integer, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		// initialize the vertex values
		Graph<Integer, HashSet<Integer>, NullValue> mappedGraph =  graph
				.mapVertices(new MapFunction<Vertex<Integer, NullValue>, HashSet<Integer>>() {

					@Override
					public HashSet<Integer> map(Vertex<Integer, NullValue> vertex) throws Exception {
						return new HashSet<Integer>();
					}
				});

		// set up the program
		Graph<Integer, HashSet<Integer>, NullValue> triangleGraph = mappedGraph.run(new TriangleCount(maxItertations));
		DataSet<Vertex<Integer, HashSet<Integer>>> triangleVertices = triangleGraph.getVertices();

		// retrieve the total number of triangles
		DataSet<Tuple1<Integer>> numberOfTriangles = triangleVertices
				.flatMap(new FlatMapFunction<Vertex<Integer, HashSet<Integer>>, Integer>() {

					@Override
					public void flatMap(Vertex<Integer, HashSet<Integer>> vertex, Collector<Integer> collector) throws Exception {

						Iterator<Integer> iteratorVertex = vertex.f1.iterator();

						if (iteratorVertex.hasNext()) {
							Integer iteratorVertexNext = iteratorVertex.next();

							if (iteratorVertexNext < 0) {
								collector.collect(iteratorVertexNext * (-1));
							}
						}
					}
				})
				.reduce(new ReduceFunction<Integer>() {

					@Override
					public Integer reduce(Integer t1, Integer t2) throws Exception {
						return t1 + t2;
					}
				}).map(new MapFunction<Integer, Tuple1<Integer>>() {
					@Override
					public Tuple1<Integer> map(Integer integer) throws Exception {
						return new Tuple1<Integer>(integer);
					}
				});

		// emit result
		if (fileOutput) {
			numberOfTriangles.writeAsCsv(outputPath, "\n", ",");
		} else {
			numberOfTriangles.print();
		}

		env.execute("Executing Triangle Count Example");

	}

	@Override
	public String getDescription() {
		return "Triangle Count Example";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxItertations = TriangleCountData.MAX_ITERATIONS;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage TriangleCount <edge path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxItertations = Integer.parseInt(args[2]);

		} else {
			System.out.println("Executing TriangleCount example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage TriangleCount <edge path> <output path> <num iterations>");
		}

		return true;
	}

	private static DataSet<Edge<Integer, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Integer.class, Integer.class)
					.map(new MapFunction<Tuple2<Integer, Integer>, Edge<Integer, NullValue>>() {
						@Override
						public Edge<Integer, NullValue> map(Tuple2<Integer, Integer> tuple2) throws Exception {
							return new Edge<Integer, NullValue>(tuple2.f0, tuple2.f1, NullValue.getInstance());
						}
					});
		} else {
			return TriangleCountData.getDefaultEdgeDataSet(env);
		}
	}

}
