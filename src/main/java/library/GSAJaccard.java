package library;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import util.DummyGraph;

import java.util.HashSet;

public class GSAJaccard {

	public static DataSet<Tuple2<String, HashSet<String>>> getVerticesWithNeighbors
			(Graph<String, HashSet<String>, NullValue> graph) {

		return graph.reduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);
	}

	public static DataSet<Tuple2<String, Tuple2<String, HashSet<String>>>> getVerticesWithNeighborsForSplitVertces
			(Graph<String, Tuple2<String, HashSet<String>>, NullValue> graph) {

		return graph.reduceOnNeighbors(new GatherNeighborsForSplitVertices(), EdgeDirection.ALL);
	}

	public static DataSet<Vertex<String, HashSet<String>>> attachValuesToVertices
			(Graph<String, HashSet<String>, NullValue> graph,
			 DataSet<Tuple2<String, HashSet<String>>> computedNeighbors) {

		return graph.joinWithVertices(computedNeighbors, new MapFunction<Tuple2<HashSet<String>, HashSet<String>>,
				HashSet<String>>() {

			@Override
			public HashSet<String> map(Tuple2<HashSet<String>, HashSet<String>> tuple2) throws Exception {
				return tuple2.f1;
			}
		}).getVertices();
	}

	public static DataSet<Vertex<String, Tuple2<String, HashSet<String>>>> attachValuesToVerticesForSplitVertices
			(Graph<String, Tuple2<String, HashSet<String>>, NullValue> graph,
			 DataSet<Tuple2<String, Tuple2<String, HashSet<String>>>> computedNeighbors) {

		return graph.joinWithVertices(computedNeighbors, new MapFunction<Tuple2<Tuple2<String, HashSet<String>>, Tuple2<String, HashSet<String>>>, Tuple2<String, HashSet<String>>>() {
			@Override
			public Tuple2<String, HashSet<String>> map(Tuple2<Tuple2<String, HashSet<String>>,
					Tuple2<String, HashSet<String>>> tuple2) throws Exception {
				return tuple2.f1;
			}
		}).getVertices();

	}

	public static DataSet<Edge<String, Double>> computeJaccard
			(Graph<String, HashSet<String>, NullValue> graphWithNeighbors) {

		return graphWithNeighbors.getTriplets().map(new ComputeJaccard());
	}

	public static DataSet<Edge<String, Double>> computeJaccardForSplitVertices
			(DummyGraph<String, Tuple2<String, HashSet<String>>, NullValue> graphWithNeighbors) {

		return graphWithNeighbors.getTriplets().map(new ComputeJaccardForSplitVertices());
	}

	/**
	 * Each vertex will have a HashSet containing its neighbors as value.
	 */
	@SuppressWarnings("serial")
	private static final class GatherNeighbors implements ReduceNeighborsFunction<HashSet<String>> {


		@Override
		public HashSet<String> reduceNeighbors(HashSet<String> first,
											   HashSet<String> second) {
			first.addAll(second);
			return new HashSet<String>(first);
		}
	}

	@SuppressWarnings("serial")
	private static final class GatherNeighborsForSplitVertices implements
			ReduceNeighborsFunction<Tuple2<String, HashSet<String>>> {

		@Override
		public Tuple2<String, HashSet<String>> reduceNeighbors(Tuple2<String, HashSet<String>> first,
															   Tuple2<String, HashSet<String>> second) {
			first.f1.addAll(second.f1);
			return first;
		}
	}

	/**
	 * Each vertex will have a HashMap containing the Jaccard coefficient for each of its values.
	 *
	 * Consider the edge x-y
	 * We denote by sizeX and sizeY, the neighbors hash set size of x and y respectively.
	 * sizeX+sizeY = union + intersection of neighborhoods
	 * size(hashSetX.addAll(hashSetY)).distinct = union of neighborhoods
	 * The intersection can then be deduced.
	 *
	 * Jaccard Similarity is then, the intersection/union.
	 */
	@SuppressWarnings("serial")
	private static final class ComputeJaccard implements
			MapFunction<Triplet<String, HashSet<String>, NullValue>, Edge<String, Double>> {

		@Override
		public Edge<String, Double> map(Triplet<String, HashSet<String>, NullValue> triplet) throws Exception {

			Vertex<String, HashSet<String>> srcVertex = triplet.getSrcVertex();
			Vertex<String, HashSet<String>> trgVertex = triplet.getTrgVertex();

			String x = srcVertex.getId();
			String y = trgVertex.getId();
			HashSet<String> neighborSetY = trgVertex.getValue();

			double unionPlusIntersection = srcVertex.getValue().size() + neighborSetY.size();
			// within a HashSet, all elements are distinct
			HashSet<String> unionSet = new HashSet<String>();
			unionSet.addAll(srcVertex.getValue());
			unionSet.addAll(neighborSetY);
			double union = unionSet.size();
			double intersection = unionPlusIntersection - union;

			return new Edge<String, Double>(x, y, intersection/union);
		}
	}

	@SuppressWarnings("serial")
	private static final class ComputeJaccardForSplitVertices implements
			MapFunction<Triplet<String, Tuple2<String, HashSet<String>>, NullValue>, Edge<String, Double>> {

		@Override
		public Edge<String, Double> map(Triplet<String, Tuple2<String, HashSet<String>>, NullValue> triplet) throws Exception {

			Vertex<String, Tuple2<String, HashSet<String>>> srcVertex = triplet.getSrcVertex();
			Vertex<String, Tuple2<String, HashSet<String>>> trgVertex = triplet.getTrgVertex();

			String x = srcVertex.getId();
			String y = trgVertex.getId();
			HashSet<String> neighborSetY = trgVertex.getValue().f1;

			double unionPlusIntersection = srcVertex.getValue().f1.size() + neighborSetY.size();
			// within a HashSet, all elements are distinct
			HashSet<String> unionSet = new HashSet<String>();
			unionSet.addAll(srcVertex.getValue().f1);
			unionSet.addAll(neighborSetY);
			double union = unionSet.size();
			double intersection = unionPlusIntersection - union;

			return new Edge<String, Double>(x, y, intersection/union);
		}
	}
}
