package example;

import example.util.TriangleCountData;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * This is an implementation of Schank and Wagner's triangle count algorithm,
 * using a gather-sum-apply iteration.
 *
 * @see <a href="http://reports-archive.adm.cs.cmu.edu/anon/ml2013/CMU-ML-13-104.pdf">page 101</a>
 */
public class GSATriangleCount implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Integer, NullValue>> edges = getEdgesDataSet(env);

		Graph<Integer, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		final Graph<Integer, NullValue, NullValue> undirectedGraph = graph.getUndirected();

		// initialize the vertex values
		Graph<Integer, HashSet<Integer>, NullValue> mappedGraph = undirectedGraph
				.mapVertices(new MapFunction<Vertex<Integer, NullValue>, HashSet<Integer>>() {
					@Override
					public HashSet<Integer> map(Vertex<Integer, NullValue> vertex) throws Exception {
						HashSet<Integer> neighbors = new HashSet<Integer>();
						neighbors.add(vertex.getId());

						return neighbors;
					}
				});

		DataSet<Vertex<Integer, HashSet<Integer>>> verticesWithNeighbors = mappedGraph
				.runGatherSumApplyIteration(new GatherNeighborIds(), new AddAllNeighbors(), new UpdateNeighbors(),
						maxItertations).getVertices();

		// scatter phase
		DataSet<Tuple1<Integer>> trianglesCountedThreeTimes = verticesWithNeighbors.join(edges).where(0).equalTo(0)
				.with(new FormTargetIdSrcValueTuple())
				.join(verticesWithNeighbors).where(0).equalTo(0)
				.with(new IntersectNeighbors());

		// each triangle is counted three times
		DataSet<Tuple1<Integer>> numberOfTriangles = trianglesCountedThreeTimes.sum(0)
				.map(new MapFunction<Tuple1<Integer>, Tuple1<Integer>>() {
					@Override
					public Tuple1<Integer> map(Tuple1<Integer> tripledValue) throws Exception {
						return new Tuple1<Integer>(tripledValue.f0/3);
					}
				});

		//emit result
		if(fileOutput) {
			numberOfTriangles.writeAsCsv(outputPath, "\n", ",");
		} else {
			numberOfTriangles.print();
		}

		env.execute("Executing GSA Triangle Count Example");
	}

	private static final class FormTargetIdSrcValueTuple implements FlatJoinFunction<Vertex<Integer,HashSet<Integer>>,
			Edge<Integer,NullValue>, Tuple2<Integer, HashSet<Integer>>> {

		@Override
		public void join(Vertex<Integer, HashSet<Integer>> vertex,
						 Edge<Integer, NullValue> edge,
						 Collector<Tuple2<Integer, HashSet<Integer>>> collector) throws Exception {

			collector.collect(new Tuple2<Integer, HashSet<Integer>>(edge.getTarget(),
					vertex.getValue()));
		}
	}

	private static final class IntersectNeighbors implements FlatJoinFunction<Tuple2<Integer,HashSet<Integer>>,
			Vertex<Integer,HashSet<Integer>>, Tuple1<Integer>> {

		@Override
		public void join(Tuple2<Integer, HashSet<Integer>> trgIdWithSrcValue,
						 Vertex<Integer, HashSet<Integer>> vertex,
						 Collector<Tuple1<Integer>> collector) throws Exception {

			HashSet<Integer> srcHashSet = trgIdWithSrcValue.f1;
			HashSet<Integer> neighborHashSet = vertex.getValue();

			// get the intersection
			int unionPlusIntersection = srcHashSet.size() + neighborHashSet.size();
			HashSet<Integer> unionSet = new HashSet<>();
			unionSet.addAll(srcHashSet);
			unionSet.addAll(neighborHashSet);

			int intersection = unionPlusIntersection - unionSet.size();

			collector.collect(new Tuple1<Integer>(intersection));
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Triangle Count UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds extends GatherFunction<HashSet<Integer>, NullValue, HashSet<Integer>> {

		@Override
		public HashSet<Integer> gather(Neighbor<HashSet<Integer>, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class AddAllNeighbors extends SumFunction<HashSet<Integer>, NullValue, HashSet<Integer>> {

		@Override
		public HashSet<Integer> sum(HashSet<Integer> newValue, HashSet<Integer> currentValue) {
			currentValue.addAll(newValue);
			return currentValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateNeighbors extends ApplyFunction<Integer, HashSet<Integer>, HashSet<Integer>> {

		@Override
		public void apply(HashSet<Integer> addedValue, HashSet<Integer> originalValue) {
			setResult(addedValue);
		}
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
				System.err.println("Usage GSATriangleCount <edge path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxItertations = Integer.parseInt(args[2]);

		} else {
			System.out.println("Executing GSATriangleCount example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage GSATriangleCount <edge path> <output path> <num iterations>");
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

	@Override
	public String getDescription() {
		return "GSA Triangle Count";
	}
}
