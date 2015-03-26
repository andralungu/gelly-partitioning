import library.CountDegree;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
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
public class AlphaNodeSplitting {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, NullValue, NullValue> initialGraph = Graph.fromDataSet(edges, env);

		long numVertices = initialGraph.numberOfVertices();

		DataSet<Tuple2<String, Long>> verticesWithDegrees = initialGraph.getDegrees();

		double avg = getAverageDegree(numVertices, verticesWithDegrees);

		DataSet<Edge<String, Tuple2<Long, Long>>> edgesWithDegrees = weighEdges(edges, verticesWithDegrees);

		DataSet<Edge<String, NullValue>> edgesWithSplitVertices = splitEdges(avg, edgesWithDegrees);

		Graph<String, Long, NullValue> graph = Graph.fromDataSet(edgesWithSplitVertices,
				new MapFunction<String, Long>() {

					@Override
					public Long map(String s) throws Exception {
						return 0L;
					}
				}, env);

		DataSet<Vertex<String, Long>> resultedVertices = graph.run(new CountDegree(maxIterations))
				.getVertices();

		DataSet<Vertex<String, Long>> aggregatedVertices = aggregatePartialValuesSplitVertices(resultedVertices);

		// emit result
		if (fileOutput) {
			aggregatedVertices.writeAsCsv(outputPath, "\n", ",");
		} else {
			aggregatedVertices.print();
		}

		env.execute("Executing Node Splitting");
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

	/**
	 * Weigh the edges with a Tuple2 containing the src/trg vertex degrees.
	 *
	 * @return
	 */
	private static DataSet<Edge<String, Tuple2<Long, Long>>> weighEdges(DataSet<Edge<String, NullValue>> edges,
																 DataSet<Tuple2<String, Long>> verticesWithDegrees) {

		return edges.join(verticesWithDegrees).where(0).equalTo(0)
				.with(new FlatJoinFunction<Edge<String, NullValue>, Tuple2<String, Long>, Edge<String, Tuple1<Long>>>() {

					@Override
					public void join(Edge<String, NullValue> edge,
									 Tuple2<String, Long> srcDegree,
									 Collector<Edge<String, Tuple1<Long>>> collector) throws Exception {

						collector.collect(new Edge<String, Tuple1<Long>>(edge.getSource(), edge.getTarget(),
								new Tuple1<Long>(srcDegree.f1)));
					}
				})
				.join(verticesWithDegrees).where(1).equalTo(0)
				.with(new FlatJoinFunction<Edge<String, Tuple1<Long>>, Tuple2<String, Long>, Edge<String, Tuple2<Long, Long>>>() {

					@Override
					public void join(Edge<String, Tuple1<Long>> edgeWithSrcDegree,
									 Tuple2<String, Long> trgDegree,
									 Collector<Edge<String, Tuple2<Long, Long>>> collector) throws Exception {

						collector.collect(new Edge<String, Tuple2<Long, Long>>(edgeWithSrcDegree.getSource(),
								edgeWithSrcDegree.getTarget(), new Tuple2<Long, Long>(edgeWithSrcDegree.getValue().f0,
								trgDegree.f1)));
					}
				});
	}

	/**
	 * We detect the skewed node by comparing its degree to the average degree.
	 * If the product alpha*average is lower than a vertex's degree, we split that vertex into alpha subvertices.
	 *
	 * The skewed vertex can be either the source or the destination of an edge. We hash its neighbor and for
	 * hashes divisible by alpha, the edge remains the same, otherwise, we add _ and a unique identifier to the vertex ID.
	 *
	 * @param avg
	 * @param edgesWithDegrees
	 * @return
	 */
	private static DataSet<Edge<String, NullValue>> splitEdges(final double avg,
															   DataSet<Edge<String, Tuple2<Long, Long>>> edgesWithDegrees) {

		return edgesWithDegrees.flatMap(new FlatMapFunction<Edge<String, Tuple2<Long, Long>>, Edge<String, NullValue>>() {

			@Override
			public void flatMap(Edge<String, Tuple2<Long, Long>> edge, Collector<Edge<String, NullValue>> collector) throws Exception {
				// if the source degree is higher than a threshold
				if (edge.getValue().f0 > alpha * avg) {
					int targetHashCode = edge.getTarget().hashCode();
					if (targetHashCode % alpha == 0) {
						collector.collect(new Edge<String, NullValue>(edge.getSource(), edge.getTarget(),
								NullValue.getInstance()));
					} else {
						collector.collect(new Edge<String, NullValue>(edge.getSource() + "_" + targetHashCode % alpha,
								edge.getTarget(), NullValue.getInstance()));
					}
				}
				// if the target degree is higher than a threshold
				else if (edge.getValue().f1 > alpha * avg) {
					int srcHashCode = edge.getSource().hashCode();
					if (srcHashCode % alpha == 0) {
						collector.collect(new Edge<String, NullValue>(edge.getSource(), edge.getTarget(),
								NullValue.getInstance()));
					} else {
						collector.collect(new Edge<String, NullValue>(edge.getSource(),
								edge.getTarget() + "_" + srcHashCode % alpha, NullValue.getInstance()));
					}
				} else {
					collector.collect(new Edge<String, NullValue>(edge.getSource(), edge.getTarget(), NullValue.getInstance()));
				}
			}
		});
	}

	/**
	 * Method that identifies the splitted vertices and adds up the partial results.
	 *
	 * @param resultedVertices the vertices resulted from the computation of the algorithm
	 * @return
	 */
	private static DataSet<Vertex<String, Long>> aggregatePartialValuesSplitVertices(DataSet<Vertex<String, Long>> resultedVertices) {

		return resultedVertices.flatMap(new FlatMapFunction<Vertex<String, Long>, Vertex<String, Long>>() {

			@Override
			public void flatMap(Vertex<String, Long> vertex, Collector<Vertex<String, Long>> collector) throws Exception {
				int pos = vertex.getId().indexOf("_");

				// if there is a splitted vertex
				if(pos > -1) {
					collector.collect(new Vertex<String, Long>(vertex.getId().substring(0, pos), vertex.getValue()));
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
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = NodeSplittingData.MAX_ITERATIONS;
	private static Integer alpha = NodeSplittingData.ALPHA;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage NodeSplittingFirstStep <edge path> <output path> <maxIterations>" +
						"<alpha>");
				return false;
			}
			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			alpha = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing NodeSplittingFirstStep with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage NodeSplittingFirstStep <edge path> <output path> <maxIterations> <alpha>");
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

