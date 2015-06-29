package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

public class CommunityDetectionData {

	// the algorithm is not guaranteed to always converge
	public static final int MAX_ITERATIONS = 30;

	public static final double DELTA = 0.5f;

	// the splitting threshold
	public static final int THRESHOLD = 6;

	public static DataSet<Edge<String, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<String, Double>> edges = new ArrayList<Edge<String, Double>>();
		edges.add(new Edge<String, Double>("1", "2", 1.0));
		edges.add(new Edge<String, Double>("1", "3", 2.0));
		edges.add(new Edge<String, Double>("1", "4", 3.0));
		edges.add(new Edge<String, Double>("2", "3", 4.0));
		edges.add(new Edge<String, Double>("2", "4", 5.0));
		edges.add(new Edge<String, Double>("3", "5", 6.0));
		edges.add(new Edge<String, Double>("5", "6", 7.0));
		edges.add(new Edge<String, Double>("5", "7", 8.0));
		edges.add(new Edge<String, Double>("6", "7", 9.0));
		edges.add(new Edge<String, Double>("7", "12", 10.0));
		edges.add(new Edge<String, Double>("8", "9", 11.0));
		edges.add(new Edge<String, Double>("8", "10", 12.0));
		edges.add(new Edge<String, Double>("8", "11", 13.0));
		edges.add(new Edge<String, Double>("9", "10", 14.0));
		edges.add(new Edge<String, Double>("9", "11", 15.0));
		edges.add(new Edge<String, Double>("10", "11", 16.0));
		edges.add(new Edge<String, Double>("10", "12", 17.0));
		edges.add(new Edge<String, Double>("11", "12", 18.0));

		return env.fromCollection(edges);
	}

	private CommunityDetectionData() {}
}