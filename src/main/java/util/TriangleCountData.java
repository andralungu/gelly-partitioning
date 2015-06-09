package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

public class TriangleCountData {

	public static final String EDGES = "1	2\n"+"1	3\n"+"2	3\n"+"2	6\n"+"3	4\n"+"3	5\n"+"3	6\n"+"4	5\n"+"6	7\n";

	public static DataSet<Edge<String, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<String, NullValue>> edges = new ArrayList<Edge<String, NullValue>>();
		edges.add(new Edge<String, NullValue>("1", "2", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1", "3", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "3", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "6", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "5", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "6", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("4", "5", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("6", "7", NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final String RESULTED_NUMBER_OF_TRIANGLES = "3";

	private TriangleCountData() {}
}
