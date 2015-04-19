import library.CountDegree;
import nl.peterbloem.powerlaws.Continuous;
import org.apache.flink.api.common.functions.CoGroupFunction;
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
import util.Sha;

import java.util.Iterator;
import java.util.List;


/**
 * The following program counts the number of degrees for each vertex, splitting vertices
 * with a high degree into subvertices.
 */
public class SortDegreeNodeSplitting {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);

		Graph<String, NullValue, NullValue> initialGraph = Graph.fromDataSet(edges, env);

		DataSet<Tuple2<String, Long>> verticesWithDegrees = initialGraph.getDegrees();

		// Data2Semantics takes an array of doubles representing the degrees, computes an
		// xMin and everything above that threshold is skewed
		DataSet<Double> degrees = verticesWithDegrees.map(new MapFunction<Tuple2<String, Long>, Double>() {

			@Override
			public Double map(Tuple2<String, Long> idDegreeTuple2) throws Exception {
				return new Double(idDegreeTuple2.f1);
			}
		});

		List<Double> arrayOfDegrees = degrees.collect();

		Continuous distribution = Continuous.fit(arrayOfDegrees).fit();

		// retrive xMin
		final double xMin = distribution.xMin();

		DataSet<Vertex<String, NullValue>> skewedVertices = verticesWithDegrees
				.flatMap(new FlatMapFunction<Tuple2<String, Long>, Vertex<String, NullValue>>() {

					@Override
					public void flatMap(Tuple2<String, Long> vertexIdDegree,
										Collector<Vertex<String, NullValue>> collector) throws Exception {
						if(vertexIdDegree.f1 > xMin) {
							collector.collect(new Vertex<String, NullValue>(vertexIdDegree.f0, NullValue.getInstance()));
						}
					}
				});

		DataSet<Edge<String, NullValue>> edgesWithSplitVertices = treeDeAggregate(skewedVertices, edges);

		Graph<String, Long, NullValue> graph = Graph.fromDataSet(edgesWithSplitVertices,
				new MapFunction<String, Long>() {

					@Override
					public Long map(String s) throws Exception {
						return 0L;
					}
				}, env);

		DataSet<Vertex<String, Long>> resultedVertices = graph.run(new CountDegree(maxIterations))
				.getVertices();

		DataSet<Vertex<String, Long>> aggregatedVertices = treeAggregate(resultedVertices);

		// emit result
		if (fileOutput) {
			aggregatedVertices.writeAsCsv(outputPath, "\n", ",");
		} else {
			aggregatedVertices.print();
		}

		env.execute("Executing Node Splitting");
	}


	/**
	 * Method that takes the skewed vertices and splits them into alpha subvertices recursively,
	 * forming a tree of `level` levels.
	 *
	 * The skewed vertex can be either the source or the destination of an edge. We hash its neighbor and add _ followed by
	 * a unique identifier to the vertex ID.
	 *
	 * @param skewedVertices
	 * @param edges - the initial data set of edges
	 *
	 * @return a dataset of edges formed from the skewed vertices and their subnodes
	 */
	private static DataSet<Edge<String, NullValue>> treeDeAggregate(DataSet<Vertex<String, NullValue>> skewedVertices,
									  DataSet<Edge<String, NullValue>> edges) {
		DataSet<Edge<String, NullValue>> edgesWithSkewedVertices = edges;

		for(int i=0; i<level; i++) {
			edgesWithSkewedVertices	= edgesWithSkewedVertices.coGroup(skewedVertices).where(0).equalTo(0)
					.with(new SplitSourceCoGroup(i))
					.coGroup(skewedVertices).where(1).equalTo(0)
					.with(new SplitTargetCoGroup(i));

			// update the skewed vertices to also contain their subvertices
			skewedVertices = skewedVertices.flatMap(new FlatMapFunction<Vertex<String, NullValue>, Vertex<String, NullValue>>() {
				@Override
				public void flatMap(Vertex<String, NullValue> vertex, Collector<Vertex<String, NullValue>> collector) throws Exception {
					for(int i =0; i < alpha; i++) {
						collector.collect(new Vertex<String, NullValue>(vertex.getId() + "_" + i, NullValue.getInstance()));
					}
				}
			});
		}

		return edgesWithSkewedVertices;
	}

	/**
	 * Method that identifies the splitted vertices and adds up the partial results.
	 *
	 * @param resultedVertices the vertices resulted from the computation of the algorithm
	 * @return
	 */
	private static DataSet<Vertex<String, Long>> treeAggregate(DataSet<Vertex<String, Long>> resultedVertices) {

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

	private static final class SplitSourceCoGroup implements
			CoGroupFunction<Edge<String, NullValue>, Vertex<String, NullValue>, Edge<String, NullValue>> {

		int currentLevel;

		public SplitSourceCoGroup(int currentLevel) {
			this.currentLevel = currentLevel;
		}

		@Override
		public void coGroup(Iterable<Edge<String, NullValue>> iterableEdges,
							Iterable<Vertex<String, NullValue>> iterableVertices,
							Collector<Edge<String, NullValue>> collector) throws Exception {
			Iterator<Vertex<String, NullValue>> vertexIterator = iterableVertices.iterator();
			Iterator<Edge<String, NullValue>> edgeIterator = iterableEdges.iterator();

			while(edgeIterator.hasNext()) {
				Edge<String, NullValue> edge = edgeIterator.next();

				int targetHashCode = edge.getTarget().hashCode();

				// avoid having the same hash code from one level to the other
				for(int i=0; i < currentLevel; i++) {
					targetHashCode = Sha.hash256(targetHashCode+"").hashCode();
				}
				if(targetHashCode < 0) {
					targetHashCode *= -1;
				}
				if (vertexIterator.hasNext()) {
					collector.collect(new Edge<String, NullValue>(vertexIterator.next().getId() + "_" + targetHashCode % alpha,
							edge.getTarget(), NullValue.getInstance()));
				} else {
					collector.collect(edge);
				}

			}
		}
	}

	private static final class SplitTargetCoGroup implements
			CoGroupFunction<Edge<String, NullValue>, Vertex<String, NullValue>, Edge<String, NullValue>> {

		int currentLevel;

		public SplitTargetCoGroup(int currentLevel) {
			this.currentLevel = currentLevel;
		}

		@Override
		public void coGroup(Iterable<Edge<String, NullValue>> iterableEdges,
							Iterable<Vertex<String, NullValue>> iterableVertices,
							Collector<Edge<String, NullValue>> collector) throws Exception {

			Iterator<Vertex<String, NullValue>> vertexIterator = iterableVertices.iterator();
			Iterator<Edge<String, NullValue>> edgeIterator = iterableEdges.iterator();

			while(edgeIterator.hasNext()) {
				Edge<String, NullValue> edge = edgeIterator.next();

				int sourceHashCode = edge.getSource().hashCode();

				// avoid having the same hash code from one level to the other
				for(int i=0; i < currentLevel; i++) {
					sourceHashCode = Sha.hash256(sourceHashCode+"").hashCode();
				}
				if(sourceHashCode < 0) {
					sourceHashCode *= -1;
				}

				if (vertexIterator.hasNext()) {
					collector.collect(new Edge<String, NullValue>(edge.getSource(),
							vertexIterator.next().getId() + "_" + sourceHashCode % alpha, NullValue.getInstance()));
				} else {
					collector.collect(edge);
				}
			}
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = NodeSplittingData.MAX_ITERATIONS;
	private static Integer alpha = NodeSplittingData.ALPHA;
	private static Integer level = NodeSplittingData.LEVEL;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 5) {
				System.err.println("Usage SortDegreeNodeSplitting <edge path> <output path> <maxIterations>" +
						"<alpha> <level>");
				return false;
			}
			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			alpha = Integer.parseInt(args[3]);
			level = Integer.parseInt(args[4]);
		} else {
			System.out.println("Executing SortDegreeNodeSplitting with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage SortDegreeNodeSplitting <edge path> <output path> <maxIterations> <alpha> <level>");
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

