package partitioning.gsa;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This class represents a <sourceVertex, edge> pair
 * This is a wrapper around Tuple2<VV, EV> for convenience in the GatherFunction
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 */
@SuppressWarnings("serial")
public class Neighbor<VV, EV> extends Tuple2<VV, EV> {

	public Neighbor() {}

	public Neighbor(VV neighborValue, EV edgeValue) {
		super(neighborValue, edgeValue);
	}

	public VV getNeighborValue() {
		return this.f0;
	}

	public EV getEdgeValue() {
		return this.f1;
	}
}
