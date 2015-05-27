package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

public class JaccardSimilarityMeasureData {

	public static final String EDGES = "1	2\n" + "1	7\n" + "2	7\n" + "3	4\n" + "3	7\n" + "4	7\n" +
			"5	6\n" + "5	7\n" + "6	7\n" + "7	8";

	public static DataSet<Edge<String, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<String, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<String, NullValue>("1", "2", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("1", "7", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("2", "7", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "4", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("3", "7", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("4", "7", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "6", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("5", "7", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("6", "7", NullValue.getInstance()));
		edges.add(new Edge<String, NullValue>("7", "8", NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final String JACCARD_VERTICES = "1,{2=0.3333333333333333, 7=0.125}\n" + "2,{1=0.3333333333333333, 7=0.125}\n"
			+"3,{4=0.3333333333333333, 7=0.125}\n" + "4,{3=0.3333333333333333, 7=0.125}\n" + "5,{6=0.3333333333333333, 7=0.125}\n"
			+"6,{5=0.3333333333333333, 7=0.125}\n" + "7,{1=0.125, 2=0.125, 3=0.125, 4=0.125, 5=0.125, 6=0.125, 8=0.0}\n"
			+"8,{7=0.0}";


	private JaccardSimilarityMeasureData() {}
}

