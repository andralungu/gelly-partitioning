package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import splitUtils.SplitVertex;
import util.ConnectedComponentsData;
import util.NodeSplittingData;

import java.util.Iterator;

public class NodeSplittingConnectedComponents implements ProgramDescription {

	public static void main (String [] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<String, NullValue>> edges = getEdgesDataSet(env);
		Graph<String, Long, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<String, Long>() {
			@Override
			public Long map(String s) throws Exception {
				return Long.parseLong(s);
			}
		}, env);

		// Step 1: Discover the skewed nodes and do the splitting (keeping the original vertex id)
		DataSet<Tuple2<String, Long>> verticesWithDegrees = graph.getDegrees();

		final double xMin = threshold;

		DataSet<Vertex<String, NullValue>> skewedVertices = SplitVertex.determineSkewedVertices(xMin,
				verticesWithDegrees);

		Graph<String, Tuple2<String, Long>, NullValue> graphWithSplitVertices =
				SplitVertex.treeDeAggregate(skewedVertices, graph, alpha, level, xMin).getUndirected();

		DataSet<Vertex<String, Tuple2<String, Long>>> splitVertices = graphWithSplitVertices.getVertices();

		// Step 2: Create a delta iteration that takes the split data set as a solution set
		// At the end of each superstep, group by vertex id(tag), do the merging and update the vertex value.
		final DeltaIteration<Vertex<String, Tuple2<String, Long>>, Vertex<String, Tuple2<String, Long>>> iteration =
				splitVertices.iterateDelta(splitVertices, maxIterations, 0);

		// perform the two regular coGroups from Vertex - centric
		DataSet<Vertex<String, Tuple2<String, Long>>> messages =  graphWithSplitVertices.getEdges()
				.coGroup(iteration.getWorkset()).where(0).equalTo(0).with(new CCMessengerMock());

		DataSet<Vertex<String, Tuple2<String, Long>>> updates = messages.coGroup(iteration.getSolutionSet())
						.where(0).equalTo(0).with(new CCUpdaterMock());

		// aggregate
		DataSet<Vertex<String, Long>> aggregatedVertices = SplitVertex.treeAggregate(updates,
				level, new Aggregate())
				.map(new MapFunction<Vertex<String, Tuple2<String, Long>>, Vertex<String, Long>>() {
					@Override
					public Vertex<String, Long> map(Vertex<String, Tuple2<String, Long>> vertex) throws Exception {
						return new Vertex<String, Long>(vertex.getId(), vertex.getValue().f1);
					}
				});

		// propagate
		DataSet<Vertex<String, Tuple2<String, Long>>> updatedSplitVertices =
				SplitVertex.propagateValuesToSplitVertices(splitVertices, aggregatedVertices);

		// close the iteration
		DataSet<Vertex<String, Tuple2<String, Long>>> partialResult = iteration.closeWith(updatedSplitVertices,
				updatedSplitVertices);

		// Step 3: Bring the vertices back to their initial state
		DataSet<Vertex<String, Long>> resultedVertices = SplitVertex.treeAggregate(partialResult,
				level, new Aggregate())
				.map(new MapFunction<Vertex<String, Tuple2<String, Long>>, Vertex<String, Long>>() {
					@Override
					public Vertex<String, Long> map(Vertex<String, Tuple2<String, Long>> vertex) throws Exception {
						return new Vertex<String, Long>(vertex.getId(), vertex.getValue().f1);
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
	private static final class CCMessengerMock implements CoGroupFunction<Edge<String, NullValue>, Vertex<String, Tuple2<String, Long>>, Vertex<String, Tuple2<String, Long>>> {

		@Override
		public void coGroup(Iterable<Edge<String, NullValue>> edges,
							Iterable<Vertex<String, Tuple2<String, Long>>> vertices,
							Collector<Vertex<String, Tuple2<String, Long>>> collector) throws Exception {

			Iterator<Edge<String, NullValue>> edgesIterator = edges.iterator();
			Iterator<Vertex<String, Tuple2<String, Long>>> verticesIterator = vertices.iterator();

			Edge<String, NullValue> edgeNext = null;
			Vertex<String, Tuple2<String, Long>> vertexNext = null;

			while (edgesIterator.hasNext()) {
				edgeNext = edgesIterator.next();

				while (verticesIterator.hasNext()) {
					vertexNext = verticesIterator.next();

					collector.collect(new Vertex<String, Tuple2<String, Long>>(edgeNext.getTarget(),
							new Tuple2<String, Long>(vertexNext.getValue().f0, vertexNext.getValue().f1)));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class CCUpdaterMock implements CoGroupFunction<Vertex<String, Tuple2<String, Long>>,
			Vertex<String, Tuple2<String, Long>>, Vertex<String, Tuple2<String, Long>>> {

		@Override
		public void coGroup(Iterable<Vertex<String, Tuple2<String, Long>>> message,
							Iterable<Vertex<String, Tuple2<String, Long>>> vertex,
							Collector<Vertex<String, Tuple2<String, Long>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, Long>>> messageIterator = message.iterator();
			Iterator<Vertex<String, Tuple2<String, Long>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, Long>> nextMessage = null;
			Vertex<String, Tuple2<String, Long>> nextVertex = null;

			long min = Long.MAX_VALUE;

			while (messageIterator.hasNext()) {
				nextMessage = messageIterator.next();

				while (vertexIterator.hasNext()) {
					nextVertex = vertexIterator.next();

					min = Math.min(min, nextMessage.getValue().f1);

					// update vertex value, if new minimum
					if(min < nextVertex.getValue().f1) {
						collector.collect(new Vertex<String, Tuple2<String, Long>>(nextVertex.getId(),
								new Tuple2<String, Long>(nextVertex.getValue().f0, min)));
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class Aggregate implements GroupReduceFunction<Vertex<String, Tuple2<String, Long>>,
			Vertex<String, Tuple2<String, Long>>> {

		@Override
		public void reduce(Iterable<Vertex<String, Tuple2<String, Long>>> vertex,
						   Collector<Vertex<String, Tuple2<String, Long>>> collector) throws Exception {

			Iterator<Vertex<String, Tuple2<String, Long>>> vertexIterator = vertex.iterator();
			Vertex<String, Tuple2<String, Long>> nextVertex = null;
			long min = Long.MAX_VALUE;

			while (vertexIterator.hasNext()) {
				nextVertex = vertexIterator.next();

				min = Math.min(min, nextVertex.getValue().f1);
				collector.collect(new Vertex<String, Tuple2<String, Long>>(nextVertex.getValue().f0,
						new Tuple2<String, Long>(nextVertex.getValue().f0, min)));
			}
		}
	}

	@Override
	public String getDescription() {
		return "Node Splitting Connected Components";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = ConnectedComponentsData.MAX_ITERATIONS;

	private static Integer alpha = NodeSplittingData.ALPHA;
	private static Integer level = NodeSplittingData.LEVEL;
	private static Integer threshold = NodeSplittingData.THRESHOLD;

	private static boolean parseParameters(String[] args) {
		if (args.length > 0) {
			if (args.length != 6) {
				System.err.println("Usage NodeSplittingConnectedComponents <edge path> <output path> " +
						"<num iterations> <alpha> <level> <threshold>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
			alpha = Integer.parseInt(args[3]);
			level = Integer.parseInt(args[4]);
			threshold = Integer.parseInt(args[5]);

		} else {
			System.out.println("Executing NodeSplittingConnectedComponents example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage ConnectedComponents <edge path> <output path> " +
					"<num iterations> <alpha> <level> <threshold>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<String, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(String.class, String.class)
					.map(new MapFunction<Tuple2<String, String>, Edge<String, NullValue>>() {
						@Override
						public Edge<String, NullValue> map(Tuple2<String, String> value) throws Exception {
							return new Edge<String, NullValue>(value.f0, value.f1, NullValue.getInstance());
						}
					});
		} else {
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}
}
