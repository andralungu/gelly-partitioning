package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import splitUtils.SplitVertex;
import util.ConnectedComponentsData;
import util.NodeSplittingData;

import java.util.Iterator;

public class NodeSplittingGSAConnectedComponents implements ProgramDescription {

	public static void main (String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgeDataSet(env);

		Graph<String, String, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<String, String>() {
			@Override
			public String map(String s) throws Exception {
				return s;
			}
		}, env);

		// Step 1: Discover the skewed nodes and do the splitting (keeping the original vertex id)
		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		final double xMin = threshold;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, String>, NullValue> graphWithSplitVertices =
				SplitVertex.treeDeAggregate(skewedVertices, graph, alpha, level, xMin).getUndirected();

		DataSet<Vertex<String, Tuple2<String, String>>> splitVertices = graphWithSplitVertices.getVertices();

		// Step 2: Create a delta iteration that takes the split data set as a solution set
		// At the end of each superstep, group by vertex id(tag), do the merging and update the vertex value.
		final DeltaIteration<Vertex<String, Tuple2<String, String>>, Vertex<String, Tuple2<String, String>>> iteration =
				splitVertices.iterateDelta(splitVertices, maxIterations, 0);

		iteration.setSolutionSetUnManaged(true);

		// perform the chain of join, map, reduce and join from GSA Iterations
		DataSet<Tuple2<String, Neighbor<Tuple2<String, String>, NullValue>>> neighbors = iteration.getWorkset()
				.join(graphWithSplitVertices.getEdges()).where(0).equalTo(0)
				.with(new FlatJoinFunction<Vertex<String, Tuple2<String, String>>, Edge<String, NullValue>, Tuple2<String,
						Neighbor<Tuple2<String, String>, NullValue>>>() {

					@Override
					public void join(Vertex<String, Tuple2<String, String>> vertex,
									 Edge<String, NullValue> edge,
									 Collector<Tuple2<String, Neighbor<Tuple2<String, String>, NullValue>>> collector) throws Exception {

						collector.collect(new Tuple2<String, Neighbor<Tuple2<String, String>, NullValue>>(
								edge.getTarget(), new Neighbor<Tuple2<String, String>, NullValue>(vertex.getValue(), NullValue.getInstance())));
					}
				});

		// gather
		DataSet<Tuple2<String, Tuple2<String, String>>> gather = neighbors.map(new GatherNeighborIds());

		// sum
		DataSet<Tuple2<String, Tuple2<String, String>>> sum = gather.groupBy(0).reduce(
				new SelectMinId());

		// apply
		DataSet<Vertex<String, Tuple2<String, String>>>  apply = sum.join(iteration.getSolutionSet())
				.where(0).equalTo(0).with(new UpdateComponentId());

		// aggregate
		DataSet<Vertex<String, String>> aggregatedVertices = SplitVertex.treeAggregate(apply,
				level, new Aggregate())
				.map(new MapFunction<Vertex<String, Tuple2<String, String>>, Vertex<String, String>>() {
					@Override
					public Vertex<String, String> map(Vertex<String, Tuple2<String, String>> vertex) throws Exception {
						return new Vertex<String, String>(vertex.getId(), vertex.getValue().f1);
					}
				});

		// propagate
		DataSet<Vertex<String, Tuple2<String, String>>> updatedSplitVertices =
				SplitVertex.propagateValuesToSplitVertices(splitVertices, aggregatedVertices);

		// close the iteration
		DataSet<Vertex<String, Tuple2<String, String>>> partialResult = iteration.closeWith(updatedSplitVertices,
				updatedSplitVertices);

		// Step 3: Bring the vertices back to their initial state
		DataSet<Vertex<String, String>> resultedVertices = SplitVertex.treeAggregate(partialResult,
				level, new Aggregate())
				.map(new MapFunction<Vertex<String, Tuple2<String, String>>, Vertex<String, String>>() {
					@Override
					public Vertex<String, String> map(Vertex<String, Tuple2<String, String>> vertex) throws Exception {
						return new Vertex<String, String>(vertex.getId(), vertex.getValue().f1);
					}
				});

		// emit result
		if (fileOutput) {
			resultedVertices.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Executing Community Detection Example");
		} else {
			resultedVertices.print();
		}
	}

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds implements
			MapFunction<Tuple2<String, Neighbor<Tuple2<String, String>, NullValue>>, Tuple2<String, Tuple2<String, String>>> {

		@Override
		public Tuple2<String, Tuple2<String, String>> map(Tuple2<String, Neighbor<Tuple2<String, String>, NullValue>> idNeighborTuple2) throws Exception {
			return new Tuple2<String, Tuple2<String, String>>(idNeighborTuple2.f0,
					idNeighborTuple2.f1.getNeighborValue());
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinId implements ReduceFunction<Tuple2<String, Tuple2<String, String>>> {

		@Override
		public Tuple2<String, Tuple2<String, String>> reduce(Tuple2<String, Tuple2<String, String>> newValue,
															 Tuple2<String, Tuple2<String, String>> currentValue) throws Exception {
			if (Long.parseLong(newValue.f1.f1) < Long.parseLong(currentValue.f1.f1)) {
				return newValue;
			}
			return currentValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateComponentId implements FlatJoinFunction<Tuple2<String,
			Tuple2<String, String>>, Vertex<String, Tuple2<String, String>>, Vertex<String, Tuple2<String, String>>> {

		@Override
		public void join(Tuple2<String, Tuple2<String, String>> summedValue,
						 Vertex<String, Tuple2<String, String>> originalValue,
						 Collector<Vertex<String, Tuple2<String, String>>> collector) throws Exception {

			if(Long.parseLong(summedValue.f1.f1) < Long.parseLong(originalValue.f1.f1)) {
				collector.collect(new Vertex<String, Tuple2<String, String>>(originalValue.f0,
						new Tuple2<String, String>(originalValue.f1.f0, summedValue.f1.f1)));
			} else {
				collector.collect(originalValue);
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class Aggregate implements GroupReduceFunction<Vertex<String, Tuple2<String, String>>,
			Vertex<String, Tuple2<String, String>>> {

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, String>>> vertex,
						   Collector<Vertex<String, Tuple2<String, String>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, String>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, String>> nextVertex = null;
			long min = Long.MAX_VALUE;

			while (vertexIterator.hasNext()) {
				nextVertex = vertexIterator.next();

				min = Math.min(min, Long.parseLong(nextVertex.getValue().f1));
				collector.collect(new Vertex<String, Tuple2<String, String>>(nextVertex.getValue().f0,
						new Tuple2<String, String>(nextVertex.getValue().f0, min+"")));
			}
		}
	}

	@Override
	public String getDescription() {
		return "Node Splitting GSA Connected Components";
	}

	// --------------------------------------------------------------------------------------------
	//  Util methods
	// --------------------------------------------------------------------------------------------

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static int maxIterations = ConnectedComponentsData.MAX_ITERATIONS;

	private static Integer alpha = NodeSplittingData.ALPHA;
	private static Integer level = NodeSplittingData.LEVEL;
	private static Integer threshold = NodeSplittingData.THRESHOLD;

	private static boolean parseParameters(String [] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;

			if (args.length != 6) {
				System.err.println("Usage: NodeSplittingGSAConnectedComponents <edge path> " +
						"<result path> <max iterations> <alpha> <level> <threshold>");
				return false;
			}

			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			alpha = Integer.parseInt(args[3]);
			level = Integer.parseInt(args[4]);
			threshold = Integer.parseInt(args[5]);
		} else {
			System.out.println("Executing NodeSplitting GSA Connected Components example with built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: GSAConnectedComponents <edge path> <result path> <max iterations>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<String, NullValue>> getEdgeDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(String.class, String.class)
					.map(new MapFunction<Tuple2<String, String>, Edge<String, NullValue>>() {

						public Edge<String, NullValue> map(Tuple2<String, String> value) throws Exception {
							return new Edge<String, NullValue>(value.f0, value.f1, NullValue.getInstance());
						}
					});
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}
}
