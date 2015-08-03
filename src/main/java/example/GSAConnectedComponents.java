package example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.gsa.*;
import org.apache.flink.types.NullValue;
import util.ConnectedComponentsData;

public class GSAConnectedComponents implements ProgramDescription {

	public static void main (String [] args) throws Exception {

		if (!parseParameters(args)) {
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

		GSAConfiguration parameters = new GSAConfiguration();
		parameters.setSolutionSetUnmanagedMemory(true);

		// Execute the GSA iteration
		Graph<String, String, NullValue> result =
				graph.runGatherSumApplyIteration(new GatherNeighborIds(), new SelectMinId(),
						new UpdateComponentId(), maxIterations, parameters);

		// emit result
		if (fileOutput) {
			result.getVertices().writeAsCsv(outputPath, "\n", " ");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("GSA Connected Components");
		} else {
			result.getVertices().print();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Connected Components UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds extends GatherFunction<String, NullValue, String> {

		public String gather(Neighbor<String, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinId extends SumFunction<String, NullValue, String> {

		public String sum(String newValue, String currentValue) {
			return Math.min(Long.parseLong(newValue), Long.parseLong(currentValue)) + "";
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateComponentId extends ApplyFunction<String, String, String> {

		public void apply(String summedValue, String origValue) {
			if (Long.parseLong(summedValue) < Long.parseLong(origValue)) {
				setResult(summedValue);
			}
		}
	}

	@Override
	public String getDescription() {
		return "GSA Connected Components";
	}

	// --------------------------------------------------------------------------------------------
	//  Util methods
	// --------------------------------------------------------------------------------------------

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static int maxIterations = ConnectedComponentsData.MAX_ITERATIONS;

	private static boolean parseParameters(String [] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;

			if (args.length != 3) {
				System.err.println("Usage: GSAConnectedComponents <edge path> " +
						"<result path> <max iterations>");
				return false;
			}

			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing GSA Connected Components example with built-in default data.");
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
