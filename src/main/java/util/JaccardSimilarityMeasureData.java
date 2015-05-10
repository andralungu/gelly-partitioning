package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

public class JaccardSimilarityMeasureData {

	public static final Integer MAX_ITERATIONS = 1;

	public static final String EDGES = "1	2\n" + "1	3\n" + "1	4\n" + "1	5\n" + "2	3\n" + "2	4\n" +
			"2	5\n" + "3	4\n" + "3	5\n" + "4	5";

	public static DataSet<Edge<Long, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<Long, NullValue>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(1L, 3L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(1L, 4L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(1L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(2L, 3L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(2L, 4L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(2L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(3L, 4L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(3L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(4L, 5L, NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final String JACCARD_VERTICES = "1,{2=0.6, 3=0.6, 4=0.6, 5=0.6}\n" + "2,{1=0.6, 3=0.6, 4=0.6, 5=0.6}\n"
			+"3,{1=0.6, 2=0.6, 4=0.6, 5=0.6}\n" + "4,{1=0.6, 2=0.6, 3=0.6, 5=0.6}\n" + "5,{1=0.6, 2=0.6, 3=0.6, 4=0.6}";


	private JaccardSimilarityMeasureData() {}
}

