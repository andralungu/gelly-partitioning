package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

public class NodeSplittingData {

	public static final Integer MAX_ITERATIONS = 25;

	public static final Integer ALPHA = 2;

	public static final Integer LEVEL = 4;

	public static final Integer THRESHOLD = 2;

	public static final String EDGES = "1	2\n" + "2	3\n" + "2	4\n" + "2	5\n" + "3	4\n" +
			"4	5\n" + "5	6\n" + "5	7\n" + "5	8\n" + "5	9\n" + "5	10\n" + "5	11\n" + "5	12\n" +
			"5	13\n" + "5	14\n" + "5	15\n" + "5	16\n" + "5	17\n" + "5	18\n" + "5	19\n" + "5	20\n" +
			"5	21";

	public static DataSet<Edge<String, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<String, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<String, NullValue>("1", "2", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "3", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "5", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("4", "5", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "6", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "7", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "8", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "9", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "10", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "11", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "12", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "13", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "14", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "15", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "16", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "17", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "18", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "19", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "20", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "21", NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final String VERTICES_WITH_DEGREES = "1,1\n" + "2,4\n" + "3,2\n" + "4,3\n" + "5,18\n" +
			"6,1\n" + "7,1\n" + "8,1\n" + "9,1\n" + "10,1\n" + "11,1\n" + "12,1\n" + "13,1\n" + "14,1\n" +
			"15,1\n" + "16,1\n" + "17,1\n" + "18,1\n" + "19,1\n" + "20,1\n" + "21,1";

	private NodeSplittingData() {}
}
