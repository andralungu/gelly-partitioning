package library;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunction;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

public class Jaccard {

	public static DataSet<Vertex<String, HashSet<String>>> getVerticesWithNeighbors
			(Graph<String, HashSet<String>, NullValue> graph) {

		return graph.groupReduceOnNeighbors(new GatherNeighbors(), EdgeDirection.ALL);
	}

	public static DataSet<Vertex<String, HashSet<String>>> getVerticesWithNeighborsForSplitVertices
			(Graph<String, Tuple2<String, HashSet<String>>, NullValue> graph) {

		return graph.groupReduceOnNeighbors(new GatherNeighborsForSplitVertices(), EdgeDirection.ALL);
	}

	public static DataSet<Vertex<String, TreeMap<String, Double>>> getVerticesWithJaccardValues
			(Graph<String, HashSet<String>, NullValue> undirectedGraphWithVertexValues) {

		return undirectedGraphWithVertexValues.groupReduceOnNeighbors(new ComputeJaccard(), EdgeDirection.ALL);
	}

	public static DataSet<Vertex<String, TreeMap<String, Double>>> getVerticesWithJaccardValuesForSplitNodes
			(Graph<String, Tuple2<String, HashSet<String>>, NullValue> undirectedGraphWithVertexValues) {

		return undirectedGraphWithVertexValues.groupReduceOnNeighbors(new ComputeJaccardForSplitVertices(), EdgeDirection.ALL);
	}

	/**
	 * Each vertex will have a HashSet containing its neighbors as value.
	 */
	@SuppressWarnings("serial")
	private static final class GatherNeighbors implements NeighborsFunction<String, HashSet<String>, NullValue,
			Vertex<String, HashSet<String>>> {

		@Override
		public void iterateNeighbors(Iterable<Tuple3<String, Edge<String, NullValue>, Vertex<String, HashSet<String>>>> neighbors,
									 Collector<Vertex<String,HashSet<String>>> collector) throws Exception {

			HashSet<String> neighborsHashSet = new HashSet<String>();
			Tuple3<String, Edge<String, NullValue>, Vertex<String, HashSet<String>>> next = null;
			Iterator<Tuple3<String, Edge<String, NullValue>, Vertex<String, HashSet<String>>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				neighborsHashSet.addAll(next.f2.getValue());
			}

			collector.collect(new Vertex<String, HashSet<String>>(next.f0,neighborsHashSet));
		}
	}

	@SuppressWarnings("serial")
	private static final class GatherNeighborsForSplitVertices implements NeighborsFunction<String, Tuple2<String, HashSet<String>>, NullValue,
			Vertex<String,HashSet<String>>> {

		@Override
		public void iterateNeighbors(Iterable<Tuple3<String, Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>>> neighbors,
										 Collector<Vertex<String, HashSet<String>>> collector) throws Exception {

			HashSet<String> neighborsHashSet = new HashSet<String>();
			Tuple3<String, Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>> next = null;
			Iterator<Tuple3<String, Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				neighborsHashSet.addAll(next.f2.getValue().f1);
			}

			collector.collect(new Vertex<String, HashSet<String>>(next.f0,neighborsHashSet));
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
	private static final class ComputeJaccard implements NeighborsFunctionWithVertexValue<String, HashSet<String>, NullValue,
			Vertex<String, TreeMap<String, Double>>> {

		@Override
		public void iterateNeighbors(Vertex<String, HashSet<String>> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, HashSet<String>>>> neighbors,
									 Collector<Vertex<String, TreeMap<String, Double>>> collector) throws Exception {

			TreeMap<String, Double> jaccard = new TreeMap<>();
			Tuple2<Edge<String, NullValue>, Vertex<String, HashSet<String>>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, HashSet<String>>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				Vertex<String, HashSet<String>> neighborVertex = next.f1;

				String y = neighborVertex.getId();
				HashSet<String> neighborSetY = neighborVertex.getValue();

				double unionPlusIntersection = vertex.getValue().size() + neighborSetY.size();
				// within a HashSet, all elements are distinct
				HashSet<String> unionSet = new HashSet<>();
				unionSet.addAll(vertex.getValue());
				unionSet.addAll(neighborSetY);
				double union = unionSet.size();
				double intersection = unionPlusIntersection - union;

				jaccard.put(y, intersection / union);
			}

			collector.collect(new Vertex<String, TreeMap<String, Double>>(vertex.getId(), jaccard));
		}
	}

	@SuppressWarnings("serial")
	private static final class ComputeJaccardForSplitVertices implements NeighborsFunctionWithVertexValue<String,
			Tuple2<String,HashSet<String>>, NullValue,
			Vertex<String, TreeMap<String, Double>>> {

		@Override
		public void iterateNeighbors(Vertex<String, Tuple2<String, HashSet<String>>> vertex,
									 Iterable<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>>> neighbors,
									 Collector<Vertex<String, TreeMap<String, Double>>>  collector) throws Exception {

			TreeMap<String, Double> jaccard = new TreeMap<>();
			Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>> next = null;
			Iterator<Tuple2<Edge<String, NullValue>, Vertex<String, Tuple2<String, HashSet<String>>>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				Vertex<String, Tuple2<String, HashSet<String>>> neighborVertex = next.f1;

				String y = neighborVertex.getValue().f0;
				HashSet<String> neighborSetY = neighborVertex.getValue().f1;

				double unionPlusIntersection = vertex.getValue().f1.size() + neighborSetY.size();
				// within a HashSet, all elements are distinct
				HashSet<String> unionSet = new HashSet<>();
				unionSet.addAll(vertex.getValue().f1);
				unionSet.addAll(neighborSetY);
				double union = unionSet.size();
				double intersection = unionPlusIntersection - union;

				jaccard.put(y, intersection / union);
			}

			collector.collect(new Vertex<String, TreeMap<String, Double>>(vertex.getId(), jaccard));
		}
	}
}
