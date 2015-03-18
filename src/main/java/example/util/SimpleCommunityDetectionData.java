package example.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

public class SimpleCommunityDetectionData {

	public static final Integer MAX_ITERATIONS = 12;

	public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, Double>> edges = new ArrayList<>();
		edges.add(new Edge<Long, Double>(1L, 2L, 1.0));
		edges.add(new Edge<Long, Double>(1L, 3L, 1.0));
		edges.add(new Edge<Long, Double>(1L, 4L, 1.0));
		edges.add(new Edge<Long, Double>(2L, 3L, 1.0));
		edges.add(new Edge<Long, Double>(2L, 4L, 1.0));
		edges.add(new Edge<Long, Double>(3L, 5L, 1.0));
		edges.add(new Edge<Long, Double>(5L, 6L, 1.0));
		edges.add(new Edge<Long, Double>(5L, 7L, 1.0));
		edges.add(new Edge<Long, Double>(6L, 7L, 1.0));
		edges.add(new Edge<Long, Double>(7L, 12L, 1.0));
		edges.add(new Edge<Long, Double>(8L, 9L, 1.0));
		edges.add(new Edge<Long, Double>(8L, 10L, 1.0));
		edges.add(new Edge<Long, Double>(8L, 11L, 1.0));
		edges.add(new Edge<Long, Double>(9L, 10L, 1.0));
		edges.add(new Edge<Long, Double>(9L, 11L, 1.0));
		edges.add(new Edge<Long, Double>(10L, 11L, 1.0));
		edges.add(new Edge<Long, Double>(10L, 12L, 1.0));
		edges.add(new Edge<Long, Double>(11L, 12L, 1.0));

		return env.fromCollection(edges);
	}

	private SimpleCommunityDetectionData() {}
}
