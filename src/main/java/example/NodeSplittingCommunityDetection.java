package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import splitUtils.SplitVertex;
import util.CommunityDetectionData;
import util.NodeSplittingData;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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

		Graph<String, Tuple2<String, Long>, Double> undirectedGraph = graphWithSplitVertices.getUndirected();

		Graph<String, Tuple2<String, Tuple2<Long, Double>>, Double> graphWithScoredVertices = undirectedGraph
				.mapVertices(new MapFunction<Vertex<String, Tuple2<String, Long>>, Tuple2<String, Tuple2<Long, Double>>>() {
					@Override
					public Tuple2<String, Tuple2<Long, Double>> map(Vertex<String, Tuple2<String, Long>> vertex) throws Exception {
						return new Tuple2<String, Tuple2<Long, Double>>(vertex.getValue().f0, new Tuple2<Long, Double>(
								vertex.getValue().f1, 1.0));
					}
				});

		DataSet<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> splitVertices = graphWithScoredVertices.getVertices();

		// Step 2: Create a delta iteration that takes the split vertex set as a solution set
		// At the end of each superstep, group by vertex id(tag), do the merging and update the vertex value.
		final DeltaIteration<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>,	Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> iteration =
				splitVertices.iterateDelta(splitVertices, maxIterations, 0);

		// perform the two regular coGroups from Vertex - centric
		DataSet<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> messages =  edges.coGroup(iteration.getWorkset())
				.where(0).equalTo(0).with(new MessagingFunctionMock());

		DataSet<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> updates =
				messages.coGroup(iteration.getSolutionSet())
				.where(0).equalTo(0).with(new VertexUpdateFunctionMock());

		// aggregate
		DataSet<Vertex<String, Tuple2<Long, Double>>> aggregatedVertices =
				SplitVertex.treeAggregate(updates, level, new Aggregate())
				.map(new MapFunction<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>,
						Map<Long, Double>>>>, Vertex<String, Tuple2<Long, Double>>>() {

					@Override
					public Vertex<String, Tuple2<Long, Double>> map(Vertex<String, Tuple2<String,
							Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> vertex) throws Exception {
						return new Vertex<String, Tuple2<Long, Double>>(vertex.getId(), new Tuple2<Long, Double>(vertex.getValue().f1.f0,
								vertex.getValue().f1.f1));
					}
				});

		// propagate
	}

	@SuppressWarnings("serial")
	public static final class MessagingFunctionMock implements CoGroupFunction<Edge<String, Double>, Vertex<String, Tuple2<String, Tuple2<Long, Double>>>,
			Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> {

		@Override
		public void coGroup(Iterable<Edge<String, Double>> edges,
							Iterable<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> vertices,
							Collector<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> collector) throws Exception {

			Iterator<Edge<String, Double>> edgesIterator = edges.iterator();
			Iterator<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> verticesIterator = vertices.iterator();

			Edge<String, Double> nextEdge = null;
			Vertex<String, Tuple2<String, Tuple2<Long, Double>>> nextVertex = null;

			while (edgesIterator.hasNext()) {
				nextEdge = edgesIterator.next();

				while (verticesIterator.hasNext()) {
					nextVertex = verticesIterator.next();

					collector.collect(new Vertex<String, Tuple2<String, Tuple2<Long, Double>>>(nextVertex.getId(),
							new Tuple2<String, Tuple2<Long, Double>>(nextVertex.getValue().f0,
									new Tuple2<Long, Double>(nextVertex.getValue().f1.f0,
									nextVertex.getValue().f1.f1 * nextEdge.getValue()))));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class VertexUpdateFunctionMock extends RichCoGroupFunction<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>,
				Vertex<String, Tuple2<String, Tuple2<Long, Double>>>, Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> {

		@Override
		public void coGroup(Iterable<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> message,
							Iterable<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> vertex,
							Collector<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> messageIterator = message.iterator();
			Iterator<Vertex<String, Tuple2<String, Tuple2<Long, Double>>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, Tuple2<Long, Double>>> messageNext = null;
			Vertex<String, Tuple2<String, Tuple2<Long, Double>>> vertexNext = null;

			// we would like these two maps to be ordered
			Map<Long, Double> receivedLabelsWithScores = new TreeMap<Long, Double>();
			Map<Long, Double> labelsWithHighestScore = new TreeMap<Long, Double>();

			while (messageIterator.hasNext()) {
				messageNext = messageIterator.next();

				while (vertexIterator.hasNext()) {
					vertexNext = vertexIterator.next();

					// split the message into received label and score
					Long receivedLabel = messageNext.getValue().f1.f0;
					Double receivedScore = messageNext.getValue().f1.f1;
					// if the label was received before
					if (receivedLabelsWithScores.containsKey(receivedLabel)) {
						Double newScore = receivedScore + receivedLabelsWithScores.get(receivedLabel);
						receivedLabelsWithScores.put(receivedLabel, newScore);
					} else {
						// first time we see the label
						receivedLabelsWithScores.put(receivedLabel, receivedScore);
					}

					// store the labels with the highest scores
					if (labelsWithHighestScore.containsKey(receivedLabel)) {
						Double currentScore = labelsWithHighestScore.get(receivedLabel);
						if (currentScore < receivedScore) {
							// record the highest score
							labelsWithHighestScore.put(receivedLabel, receivedScore);
						}
					} else {
						// first time we see this label
						labelsWithHighestScore.put(receivedLabel, receivedScore);
					}
				}
			}

			// keep the TreeMaps in the vertex value
			collector.collect(new Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>(
							vertexNext.getId(), new Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>(
							vertexNext.getValue().f0, new Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>(
							vertexNext.getValue().f1.f0, vertexNext.getValue().f1.f1,
							receivedLabelsWithScores, labelsWithHighestScore
					))));
		}
	}

////							// else delta = 0
////							// update own label
////							collector.collect(new Vertex<String, Tuple3<String, Long, Double>>(vertexNext.getId(),
////									new Tuple3<String, Long, Double>(vertexNext.getValue().f0,
////											maxScoreLabel, highestScore)));
////						}
//						collector.collect(new Vertex<String, Tuple3<String, Long, Double>>(nextVertex.getValue().f0,
//								new Tuple3<String, Long, Double>(nextVertex.getValue().f0, nextVertex.getValue().f1, nextVertex.getValue().f2)));
//					}
//				});

	@SuppressWarnings("serial")
	public static final class Aggregate extends RichGroupReduceFunction<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>,
			Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> {


		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> vertexIterable,
						   Collector<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>> iteratorVertex = vertexIterable.iterator();
			Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>> nextVertex = null;

			while (iteratorVertex.hasNext()) {
				nextVertex = iteratorVertex.next();

				Map<Long, Double> receivedLabelsWithScores = nextVertex.getValue().f1.f2;
				Map<Long, Double> labelsWithHighestScore = nextVertex.getValue().f1.f3;

				if (receivedLabelsWithScores.size() > 0) {
					// find the label with the highest score from the ones received
					Double maxScore = -Double.MAX_VALUE;
					Long maxScoreLabel = nextVertex.getValue().f1.f0;
					for (Long curLabel : receivedLabelsWithScores.keySet()) {
						if (receivedLabelsWithScores.get(curLabel) > maxScore) {
							maxScore = receivedLabelsWithScores.get(curLabel);
							maxScoreLabel = curLabel;
						}
					}
					// find the highest score of maxScoreLabel
					Double highestScore = labelsWithHighestScore.get(maxScoreLabel);
					// re-score the new label
					if (maxScoreLabel != nextVertex.getValue().f1.f0) {
						highestScore -= delta / getIterationRuntimeContext().getSuperstepNumber();
					}
					// else delta = 0
					// update own label
					// RichGroupReduce wants you to return the same type
					collector.collect(new Vertex<String, Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>>(
							nextVertex.getValue().f0,
							new Tuple2<String, Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>>(
									nextVertex.getValue().f0, new Tuple4<Long, Double, Map<Long, Double>, Map<Long, Double>>(
									maxScoreLabel, highestScore, new TreeMap<Long, Double>(), new TreeMap<Long, Double>()))));
				}
			}
		}
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
