package example;

import example.util.JaccardSimilarityMeasureData;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
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

import java.util.HashSet;
import java.util.Iterator;

public class JaccardSimilarityMeasureExample implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		Graph<Long, NullValue, Double> graph = Graph.fromDataSet(edges, env);
		// undirect the graph
		Graph<Long, NullValue, Double> undirectedGraph = graph.getUndirected();

		DataSet<Vertex<Long, HashSet<Long>>> verticesWithNeighbors = assignNeighborsAsValues(undirectedGraph.getEdges());

		DataSet<Edge<Long, Double>> result = computeJaccardSimilarity(verticesWithNeighbors);

		// emit result
		if (fileOutput) {
			result.writeAsCsv(outputPath, "\n", ",");
		} else {
			result.print();
		}

		env.execute("Executing Jaccard Similarity Measure");
	}

	@Override
	public String getDescription() {
		return "Vertex Jaccard Similarity Measure";
	}

	/**
	 * Each vertex will have a HashSet containing its neighbors as value.
	 *
	 * @param edges
	 * @return
	 */
	private static DataSet<Vertex<Long, HashSet<Long>>> assignNeighborsAsValues(DataSet<Edge<Long, Double>> edges) {

		return edges.groupBy(0).reduceGroup(new GroupReduceFunction<Edge<Long, Double>, Vertex<Long, HashSet<Long>>>() {

			@Override
			public void reduce(Iterable<Edge<Long, Double>> iterableEdge,
							   Collector<Vertex<Long, HashSet<Long>>> collector) throws Exception {

				Iterator<Edge<Long, Double>> iteratorEdge = iterableEdge.iterator();
				HashSet<Long> neighbors = new HashSet<Long>();
				Long vertexSrcId = 0L;

				while(iteratorEdge.hasNext()) {
					Edge<Long, Double> iteratorEdgeNext = iteratorEdge.next();
					vertexSrcId = iteratorEdgeNext.getSource();
					neighbors.add(iteratorEdgeNext.getTarget());
				}

				collector.collect(new Vertex<Long, HashSet<Long>>(vertexSrcId, neighbors));
			}
		});
	}

	/**
	 * Consider the edge x-y
	 * We denote by sizeX and sizeY, the neighbors hash set size of x and y respectively.
	 * sizeX+sizeY = union + intersection of neighborhoods
	 * size(hashSetX.addAll(hashSetY)).distinct = union of neighborhoods
	 * The intersection can then be deduced.
	 *
	 * Jaccard Similarity is then, the intersection/union.
	 *
	 * @param verticesWithNeighbors
	 * @return
	 */
	private static DataSet<Edge<Long, Double>> computeJaccardSimilarity(
			DataSet<Vertex<Long, HashSet<Long>>> verticesWithNeighbors) {

		return verticesWithNeighbors.cross(verticesWithNeighbors).with(new CrossFunction<Vertex<Long, HashSet<Long>>,
				Vertex<Long, HashSet<Long>>, Edge<Long, Double>>() {

			@Override
			public Edge<Long, Double> cross(Vertex<Long, HashSet<Long>> vertexX,
											Vertex<Long, HashSet<Long>> vertexY) throws Exception {

				int sizeX = vertexX.getValue().size();
				int sizeY = vertexY.getValue().size();

				double unionPlusIntersection = sizeX + sizeY;
				// within a HashSet, all elements are distinct
				vertexX.getValue().addAll(vertexY.getValue());
				//vertexX.value contains the union
				double union = vertexX.getValue().size();
				double intersection = unionPlusIntersection - union;

				return new Edge<Long, Double>(vertexX.getId(), vertexY.getId(), intersection/union);
			}
		}).filter(new FilterFunction<Edge<Long, Double>>() {
			@Override
			public boolean filter(Edge<Long, Double> edge) throws Exception {
				return !edge.getSource().equals(edge.getTarget());
			}
		});
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
				System.err.println("Usage JaccardSimilarityMeasureExample <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing JaccardSimilarityMeasure example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage JaccardSimilarityMeasureExample <edge path> <output path>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, Double>>() {
						@Override
						public Edge<Long, Double> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<Long, Double>(tuple2.f0, tuple2.f1, new Double(0));
						}
					});
		} else {
			return JaccardSimilarityMeasureData.getDefaultEdgeDataSet(env);
		}
	}
}
