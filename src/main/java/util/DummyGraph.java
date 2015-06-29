package util;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.NeighborsFunction;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.Iterator;

/**
 * Class that simply adds join hints to Gelly's groupReduceOn* methods
 *
 * @param <K>
 * @param <VV>
 * @param <EV>
 */
public class DummyGraph<K, VV, EV> {

	private final ExecutionEnvironment context;
	private final DataSet<Vertex<K, VV>> vertices;
	private final DataSet<Edge<K, EV>> edges;

	private DummyGraph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
		this.context = context;
	}


	/**
	 * @return the vertex DataSet.
	 */
	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}

	/**
	 * @return the edge DataSet.
	 */
	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
	}

	public static <K, EV> DummyGraph<K, NullValue, EV> fromDataSet(
			DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		DataSet<Vertex<K, NullValue>> vertices = edges.flatMap(new EmitSrcAndTarget<K, EV>()).distinct();
		return new DummyGraph<K, NullValue, EV>(vertices, edges, context);
	}
	private static final class EmitSrcAndTarget<K, EV> implements FlatMapFunction<
			Edge<K, EV>, Vertex<K, NullValue>> {
		public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, NullValue>> out) {
			out.collect(new Vertex<K, NullValue>(edge.f0, NullValue.getInstance()));
			out.collect(new Vertex<K, NullValue>(edge.f1, NullValue.getInstance()));
		}
	}

	public static <K, VV, EV> DummyGraph<K, VV, EV> fromDataSet(DataSet<Edge<K, EV>> edges,
																final MapFunction<K, VV> mapper, ExecutionEnvironment context) {
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);
		TypeInformation<VV> valueType = TypeExtractor.createTypeInfo(
				MapFunction.class, mapper.getClass(), 1, null, null);
		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<K, VV>> returnType = (TypeInformation<Vertex<K, VV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);
		DataSet<Vertex<K, VV>> vertices = edges
				.flatMap(new EmitSrcAndTargetAsTuple1<K, EV>()).distinct()
				.map(new MapFunction<Tuple1<K>, Vertex<K, VV>>() {
					public Vertex<K, VV> map(Tuple1<K> value) throws Exception {
						return new Vertex<K, VV>(value.f0, mapper.map(value.f0));
					}
				}).returns(returnType).withForwardedFields("f0");
		return new DummyGraph<K, VV, EV>(vertices, edges, context);
	}
	private static final class EmitSrcAndTargetAsTuple1<K, EV> implements FlatMapFunction<
			Edge<K, EV>, Tuple1<K>> {
		public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) {
			out.collect(new Tuple1<K>(edge.f0));
			out.collect(new Tuple1<K>(edge.f1));
		}
	}

	public static <K, VV, EV> DummyGraph<K, VV, EV> fromDataSet(DataSet<Vertex<K, VV>> vertices,
																DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		return new DummyGraph<K, VV, EV>(vertices, edges, context);
	}

	public DummyGraph<K, VV, EV> getUndirected() {
		DataSet<Edge<K, EV>> undirectedEdges = edges.flatMap(new RegularAndReversedEdgesMap<K, EV>());
		return new DummyGraph<K, VV, EV>(vertices, undirectedEdges, this.context);
	}

	private static final class RegularAndReversedEdgesMap<K, EV>
			implements FlatMapFunction<Edge<K, EV>, Edge<K, EV>> {
		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out) throws Exception {
			out.collect(new Edge<K, EV>(edge.f0, edge.f1, edge.f2));
			out.collect(new Edge<K, EV>(edge.f1, edge.f0, edge.f2));
		}
	}

	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors has access to the vertex
	 * value.
	 *
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
												 EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
			case IN:
				// create <edge-sourceVertex> pairs
				DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
						.join(this.vertices).where(0).equalTo(0);
				return vertices.coGroup(edgesWithSources)
						.where(0).equalTo("f0.f1")
						.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction));
			case OUT:
				// create <edge-targetVertex> pairs
				DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
						.join(this.vertices).where(1).equalTo(0);
				return vertices.coGroup(edgesWithTargets)
						.where(0).equalTo("f0.f0")
						.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction));
			case ALL:
				// create <edge-sourceOrTargetVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
						.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
						.join(this.vertices, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(1).equalTo(0)
						.with(new ProjectEdgeWithNeighbor<K, VV, EV>());
				return vertices.coGroup(edgesWithNeighbors)
						.where(0).equalTo(0)
						.with(new ApplyCoGroupFunctionOnAllNeighbors<K, VV, EV, T>(neighborsFunction));
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors only has access to the
	 * vertex id (not the vertex value).
	 *
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
												 EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
			case IN:
				// create <edge-sourceVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
						.join(this.vertices).where(0).equalTo(0)
						.with(new ProjectVertexIdJoin<K, VV, EV>(1))
						.withForwardedFieldsFirst("f1->f0");
				return edgesWithSources.groupBy(0).reduceGroup(
						new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
			case OUT:
				// create <edge-targetVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectVertexIdJoin<K, VV, EV>(0))
						.withForwardedFieldsFirst("f0");
				return edgesWithTargets.groupBy(0).reduceGroup(
						new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
			case ALL:
				// create <edge-sourceOrTargetVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
						.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
						.join(this.vertices, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND).where(1).equalTo(0)
						.with(new ProjectEdgeWithNeighbor<K, VV, EV>());
				return edgesWithNeighbors.groupBy(0).reduceGroup(
						new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Compute an aggregate over the neighbor values of each
	 * vertex.
	 *
	 * @param reduceNeighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @return a Dataset containing one value per vertex (vertex id, aggregate vertex value)
	 * @throws IllegalArgumentException
	 */
	public DataSet<Tuple2<K, VV>> reduceOnNeighbors(ReduceNeighborsFunction<VV> reduceNeighborsFunction,
													EdgeDirection direction,
													File computationFile) throws IllegalArgumentException {
		switch (direction) {
			case IN:
				// create <vertex-source value> pairs
				final DataSet<Tuple2<K, VV>> verticesWithSourceNeighborValues = edges
						.join(this.vertices, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(0).equalTo(0)
						.with(new ProjectVertexWithNeighborValueJoin<K, VV, EV>(1))
						.withForwardedFieldsFirst("f1->f0");
				return verticesWithSourceNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
						reduceNeighborsFunction, computationFile));
			case OUT:
				// create <vertex-target value> pairs
				DataSet<Tuple2<K, VV>> verticesWithTargetNeighborValues = edges
						.join(this.vertices, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(1).equalTo(0)
						.with(new ProjectVertexWithNeighborValueJoin<K, VV, EV>(0))
						.withForwardedFieldsFirst("f0");
				return verticesWithTargetNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
						reduceNeighborsFunction, computationFile));
			case ALL:
				// create <vertex-neighbor value> pairs
				DataSet<Tuple2<K, VV>> verticesWithNeighborValues = edges
						.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
						.join(this.vertices, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(1).equalTo(0)
						.with(new ProjectNeighborValue<K, VV, EV>());

				return verticesWithNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
						reduceNeighborsFunction, computationFile));
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	private static final class ProjectVertexWithNeighborValueJoin<K, VV, EV>
			implements FlatJoinFunction<Edge<K, EV>, Vertex<K, VV>, Tuple2<K, VV>> {

		private int fieldPosition;

		public ProjectVertexWithNeighborValueJoin(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public void join(Edge<K, EV> edge, Vertex<K, VV> otherVertex,
						 Collector<Tuple2<K, VV>> out) {
			out.collect(new Tuple2<K, VV>((K) edge.getField(fieldPosition), otherVertex.getValue()));
		}
	}

	private static final class ProjectVertexIdJoin<K, VV, EV> implements FlatJoinFunction<
			Edge<K, EV>, Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> {

		private int fieldPosition;

		public ProjectVertexIdJoin(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public void join(Edge<K, EV> edge, Vertex<K, VV> otherVertex,
						 Collector<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> out) {
			out.collect(new Tuple3<K, Edge<K, EV>, Vertex<K, VV>>((K) edge.getField(fieldPosition), edge, otherVertex));
		}
	}

	private static final class ProjectNeighborValue<K, VV, EV> implements FlatJoinFunction<
			Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple2<K, VV>> {

		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
						 Collector<Tuple2<K, VV>> out) {

			out.collect(new Tuple2<K, VV>(keysWithEdge.f0, neighbor.getValue()));
		}
	}

	private static final class ApplyNeighborReduceFunction<K, VV> implements ReduceFunction<Tuple2<K, VV>> {

		private ReduceNeighborsFunction<VV> function;
		private File computationTempFile;

		public ApplyNeighborReduceFunction(ReduceNeighborsFunction<VV> fun, File computationTempFile) {
			this.function = fun;
			this.computationTempFile = computationTempFile;
		}

		@Override
		public Tuple2<K, VV> reduce(Tuple2<K, VV> first, Tuple2<K, VV> second) throws Exception {
			long start = System.currentTimeMillis();
			first.setField(function.reduceNeighbors(first.f1, second.f1), 1);
			long stop = System.currentTimeMillis();
			long time = stop - start;
			String updateTimeElapsed = "Vertex key " + first.f0 +
					" time elapsed computation " + time + "\n";
			Files.append(updateTimeElapsed, computationTempFile, Charsets.UTF_8);
			return first;
		}
	}

	private static final class ApplyNeighborCoGroupFunction<K, VV, EV, T> implements CoGroupFunction<
			Vertex<K, VV>, Tuple2<Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {
		private NeighborsFunctionWithVertexValue<K, VV, EV, T> function;
		public ApplyNeighborCoGroupFunction(NeighborsFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}
		public void coGroup(Iterable<Vertex<K, VV>> vertex, Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighbors,
							Collector<T> out) throws Exception {
			Iterator<Vertex<K, VV>> iterator = vertex.iterator();
			if (iterator.hasNext()) {
				function.iterateNeighbors(iterator.next(), neighbors, out);
			}
		}
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class, function.getClass(), 3, null, null);
		}
	}

	public <T> DummyGraph<K, VV, EV> joinWithVerticesTuple2(DataSet<Tuple2<K, T>> inputDataSet,
												 final MapFunction<Tuple2<VV, T>, VV> mapper) {

		DataSet<Vertex<K, VV>> resultedVertices = this.getVertices()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToVertexValuesTuple2<K, VV, T>(mapper));
		return new DummyGraph<K, VV, EV>(resultedVertices, this.edges, this.context);
	}

	private static final class ApplyCoGroupToVertexValuesTuple2<K, VV, T>
			implements CoGroupFunction<Vertex<K, VV>, Tuple2<K, T>, Vertex<K, VV>> {

		private MapFunction<Tuple2<VV, T>, VV> mapper;

		public ApplyCoGroupToVertexValuesTuple2(MapFunction<Tuple2<VV, T>, VV> mapper) {
			this.mapper = mapper;
		}

		@Override
		public void coGroup(Iterable<Vertex<K, VV>> vertices,
							Iterable<Tuple2<K, T>> input, Collector<Vertex<K, VV>> collector) throws Exception {

			final Iterator<Vertex<K, VV>> vertexIterator = vertices.iterator();
			final Iterator<Tuple2<K, T>> inputIterator = input.iterator();

			if (vertexIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Tuple2<K, T> inputNext = inputIterator.next();

					collector.collect(new Vertex<K, VV>(inputNext.f0, mapper
							.map(new Tuple2<VV, T>(vertexIterator.next().f1,
									inputNext.f1))));
				} else {
					collector.collect(vertexIterator.next());
				}

			}
		}
	}

	private final class ProjectEdgeWithSrcValue<K, VV, EV> implements
			FlatJoinFunction<Vertex<K, VV>, Edge<K, EV>, Tuple4<K, K, VV, EV>> {

		@Override
		public void join(Vertex<K, VV> vertex, Edge<K, EV> edge, Collector<Tuple4<K, K, VV, EV>> collector)
				throws Exception {

			collector.collect(new Tuple4<K, K, VV, EV>(edge.getSource(), edge.getTarget(), vertex.getValue(),
					edge.getValue()));
		}
	}

	private static final class EmitOneEdgeWithNeighborPerNode<K, EV> implements FlatMapFunction<
			Edge<K, EV>, Tuple3<K, K, Edge<K, EV>>> {
		public void flatMap(Edge<K, EV> edge, Collector<Tuple3<K, K, Edge<K, EV>>> out) {
			out.collect(new Tuple3<K, K, Edge<K, EV>>(edge.getSource(), edge.getTarget(), edge));
			out.collect(new Tuple3<K, K, Edge<K, EV>>(edge.getTarget(), edge.getSource(), edge));
		}
	}

	private static final class ProjectEdgeWithNeighbor<K, VV, EV> implements FlatJoinFunction<
			Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> {
		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
						 Collector<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> out) {
			out.collect(new Tuple3<K, Edge<K, EV>, Vertex<K, VV>>(keysWithEdge.f0, keysWithEdge.f2, neighbor));
		}
	}

	private static final class ApplyCoGroupFunctionOnAllNeighbors<K, VV, EV, T>
			implements CoGroupFunction<Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {
		private NeighborsFunctionWithVertexValue<K, VV, EV, T> function;
		public ApplyCoGroupFunctionOnAllNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}
		public void coGroup(Iterable<Vertex<K, VV>> vertex,
							final Iterable<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> keysWithNeighbors,
							Collector<T> out) throws Exception {
			final Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighborsIterator = new Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>>() {
				final Iterator<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> keysWithEdgesIterator = keysWithNeighbors.iterator();
				@Override
				public boolean hasNext() {
					return keysWithEdgesIterator.hasNext();
				}
				@Override
				public Tuple2<Edge<K, EV>, Vertex<K, VV>> next() {
					Tuple3<K, Edge<K, EV>, Vertex<K, VV>> next = keysWithEdgesIterator.next();
					return new Tuple2<Edge<K, EV>, Vertex<K, VV>>(next.f1, next.f2);
				}
				@Override
				public void remove() {
					keysWithEdgesIterator.remove();
				}
			};
			Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighborsIterable = new Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>>() {
				public Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>> iterator() {
					return neighborsIterator;
				}
			};

			Iterator<Vertex<K,VV>> vertexIterator = vertex.iterator();

			if(vertexIterator.hasNext()) {
				function.iterateNeighbors(vertexIterator.next(), neighborsIterable, out);
			}
		}
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class, function.getClass(), 3, null, null);
		}
	}

	private static final class ApplyNeighborGroupReduceFunction<K, VV, EV, T>
			implements GroupReduceFunction<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {
		private NeighborsFunction<K, VV, EV, T> function;
		public ApplyNeighborGroupReduceFunction(NeighborsFunction<K, VV, EV, T> fun) {
			this.function = fun;
		}
		public void reduce(Iterable<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> vertex, Collector<T> out) throws Exception {
			Iterator<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> vertexIterator = vertex.iterator();

			if(vertexIterator.hasNext()) {
				function.iterateNeighbors(vertex, out);
			}


		}
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunction.class, function.getClass(), 3, null, null);
		}
	}

	public DataSet<Triplet<K, VV, EV>> getTriplets() {
		return this.getVertices().join(this.getEdges()).where(0).equalTo(0)
				.with(new ProjectEdgeWithSrcValue<K, VV, EV>())
				.join(this.getVertices()).where(1).equalTo(0)
				.with(new ProjectEdgeWithVertexValues<K, VV, EV>());
	}

	private static final class ProjectEdgeWithVertexValues<K, VV, EV> implements
			FlatJoinFunction<Tuple4<K, K, VV, EV>, Vertex<K, VV>, Triplet<K, VV, EV>> {

		@Override
		public void join(Tuple4<K, K, VV, EV> tripletWithSrcValSet,
						 Vertex<K, VV> vertex, Collector<Triplet<K, VV, EV>> collector) throws Exception {

			collector.collect(new Triplet<K, VV, EV>(tripletWithSrcValSet.f0, tripletWithSrcValSet.f1,
					tripletWithSrcValSet.f2, vertex.getValue(), tripletWithSrcValSet.f3));
		}
	}

	/**
	 * Return the degree of all vertices in the graph
	 *
	 * @return A DataSet of Tuple2<vertexId, degree>
	 */
	public DataSet<Tuple2<K, Long>> getDegrees() {
		return outDegrees().union(inDegrees()).groupBy(0).sum(1);
	}

	public DataSet<Tuple2<K, Long>> outDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(0).with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	private static final class CountNeighborsCoGroup<K, VV, EV>
			implements CoGroupFunction<Vertex<K, VV>, Edge<K, EV>, Tuple2<K, Long>> {
		@SuppressWarnings("unused")
		public void coGroup(Iterable<Vertex<K, VV>> vertex,	Iterable<Edge<K, EV>> outEdges,
							Collector<Tuple2<K, Long>> out) {
			long count = 0;
			for (Edge<K, EV> edge : outEdges) {
				count++;
			}

			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();

			if(vertexIterator.hasNext()) {
				out.collect(new Tuple2<K, Long>(vertexIterator.next().f0, count));
			}
		}
	}

	/**
	 * Return the in-degree of all vertices in the graph
	 *
	 * @return A DataSet of Tuple2<vertexId, inDegree>
	 */
	public DataSet<Tuple2<K, Long>> inDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(1).with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	public long numberOfVertices() throws Exception {
		return vertices.count();
	}

	/**
	 * Apply a function to the attribute of each vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <NV> DummyGraph<K, NV, EV> mapVertices(final MapFunction<Vertex<K, VV>, NV> mapper) {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);

		TypeInformation<NV> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, null, null);

		TypeInformation<Vertex<K, NV>> returnType = (TypeInformation<Vertex<K, NV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);

		DataSet<Vertex<K, NV>> mappedVertices = vertices.map(
				new MapFunction<Vertex<K, VV>, Vertex<K, NV>>() {
					public Vertex<K, NV> map(Vertex<K, VV> value) throws Exception {
						return new Vertex<K, NV>(value.f0, mapper.map(value));
					}
				})
				.returns(returnType)
				.withForwardedFields("f0");

		return new DummyGraph<K, NV, EV>(mappedVertices, this.edges, this.context);
	}

	/**
	 * Joins the vertex DataSet of this graph with an input DataSet and applies
	 * a UDF on the resulted values.
	 *
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @return a new graph where the vertex values have been updated.
	 */
	public <T> DummyGraph<K, VV, EV> joinWithVertices(DataSet<Vertex<K, T>> inputDataSet,
												 final MapFunction<Vertex<VV, T>, VV> mapper) {

		DataSet<Vertex<K, VV>> resultedVertices = this.getVertices()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToVertexValues<K, VV, T>(mapper));
		return new DummyGraph<K, VV, EV>(resultedVertices, this.edges, this.context);
	}

	private static final class ApplyCoGroupToVertexValues<K, VV, T>
			implements CoGroupFunction<Vertex<K, VV>, Vertex<K, T>, Vertex<K, VV>> {

		private MapFunction<Vertex<VV, T>, VV> mapper;

		public ApplyCoGroupToVertexValues(MapFunction<Vertex<VV, T>, VV> mapper) {
			this.mapper = mapper;
		}

		@Override
		public void coGroup(Iterable<Vertex<K, VV>> vertices,
							Iterable<Vertex<K, T>> input, Collector<Vertex<K, VV>> collector) throws Exception {

			final Iterator<Vertex<K, VV>> vertexIterator = vertices.iterator();
			final Iterator<Vertex<K, T>> inputIterator = input.iterator();

			if (vertexIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Vertex<K, T> inputNext = inputIterator.next();

					collector.collect(new Vertex<K, VV>(inputNext.f0, mapper
							.map(new Vertex<VV, T>(vertexIterator.next().f1,
									inputNext.f1))));
				} else {
					collector.collect(vertexIterator.next());
				}

			}
		}
	}

}
