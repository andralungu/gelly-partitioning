package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

public class ConnectedComponentsData {

	public static final Integer MAX_ITERATIONS = 4;

	public static final String EDGES = "1	2\n" + "2	3\n" + "2	4\n" + "3	4";

	public static DataSet<Edge<String, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {
		List<Edge<String, NullValue>> edges = new ArrayList<Edge<String, NullValue>>();
		edges.add(new Edge<String, NullValue>("1", "2", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "3", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "4", NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final String VERTICES_WITH_MIN_ID = "1,1\n" + "2,1\n" + "3,1\n" + "4,1";

	private ConnectedComponentsData() {}
}
