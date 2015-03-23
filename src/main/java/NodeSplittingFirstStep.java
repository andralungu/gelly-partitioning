import library.CountDegree;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import util.NodeSplittingData;

import java.util.Iterator;


/**
 * The following program counts the number of degrees for each vertex, splitting vertices
 * with a high degree into subvertices.
 */
public class NodeSplittingFirstStep {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		// We already know that vertex 5 has the highest degree, so we will split it into two subvertices
		// if the edge we are looking for is either the src or the dst, we hash its neighbor id and
		// for even hashes, the edge remains intact, for odd hashes, we create a new vertex having the id prevIDb
		DataSet<Edge<String, NullValue>> edgesWithSplitVertices = edges
				.flatMap(new FlatMapFunction<Edge<String, NullValue>, Edge<String, NullValue>>() {

					@Override
					public void flatMap(Edge<String, NullValue> edge, Collector<Edge<String, NullValue>> collector) throws Exception {
						if(edge.getSource().equals("5")) {
							int targetHashCode = edge.getTarget().hashCode();
							// even
							if(targetHashCode % 2 == 0) {
								collector.collect(edge);
							} else {
								// odd
								collector.collect(new Edge<String, NullValue>(edge.getSource()+"b",
										edge.getTarget(), NullValue.getInstance()));
							}
						}
						else if(edge.getTarget().equals("5")) {
							int srcHashCode = edge.getSource().hashCode();
							// even
							if(srcHashCode % 2 == 0) {
								collector.collect(edge);
							} else {
								// odd
								collector.collect(new Edge<String, NullValue>(edge.getSource(),
										edge.getTarget()+"b", NullValue.getInstance()));
							}
						}
						else {
							collector.collect(edge);
						}
					}
				});

		Graph<String, NullValue, NullValue> graph = Graph.fromDataSet(edgesWithSplitVertices, env);

		// initialize the vertex values
		Graph<String, Long, NullValue> mappedGraph = graph
				.mapVertices(new MapFunction<Vertex<String, NullValue>, Long>() {

					@Override
					public Long map(Vertex<String, NullValue> vertex) throws Exception {
						return 0L;
					}
				});

		// set up the program
		Graph<String, Long, NullValue> result = mappedGraph.run(new CountDegree(maxIterations));

		DataSet<Vertex<String, Long>> resultedVertices = result.getVertices();

		// add up 5's partial result with 5b's partial result
		DataSet<Vertex<String, Long>> aggregatedVertices = resultedVertices
				.flatMap(new FlatMapFunction<Vertex<String, Long>, Vertex<String, Long>>() {

					@Override
					public void flatMap(Vertex<String, Long> vertex, Collector<Vertex<String, Long>> collector) throws Exception {

						if(vertex.getId().equals("5b")) {
							collector.collect(new Vertex<String, Long>("5", vertex.getValue()));
						} else {
							collector.collect(vertex);
						}
					}
				}).groupBy(0).reduceGroup(new GroupReduceFunction<Vertex<String, Long>, Vertex<String, Long>>() {

					@Override
					public void reduce(Iterable<Vertex<String, Long>> iterable,
									   Collector<Vertex<String, Long>> collector) throws Exception {
						long sum = 0;
						Vertex<String, Long> vertex = new Vertex<String, Long>();

						Iterator<Vertex<String, Long>> iterator = iterable.iterator();
						while (iterator.hasNext()) {
							vertex = iterator.next();
							sum += vertex.getValue();
						}

						collector.collect(new Vertex<String, Long>(vertex.getId(), sum));
					}
				});

		// emit result
		if (fileOutput) {
			aggregatedVertices.writeAsCsv(outputPath, "\n", ",");
		} else {
			aggregatedVertices.print();
		}

		env.execute("Executing Node Splitting");
	}



	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = NodeSplittingData.MAX_ITERATIONS;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage NodeSplittingFirstStep <edge path> <output path> <maxIterations>");
				return false;
			}
			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing NodeSplittingFirstStep with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage NodeSplittingFirstStep <edge path> <output path> <maxIterations>");
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
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<String, NullValue>>() {

						@Override
						public Edge<String, NullValue> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<String, NullValue>(tuple2.f0.toString(), tuple2.f1.toString(), NullValue.getInstance());
						}
					});
		} else {
			return NodeSplittingData.getDefaultEdgeDataSet(env);
		}
	}
}

