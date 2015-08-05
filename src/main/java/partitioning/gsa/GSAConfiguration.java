package partitioning.gsa;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.IterationConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * A GSAConfiguration object can be used to set the iteration name and
 * degree of parallelism, to register aggregators and use broadcast sets in
 * the {@link org.apache.flink.graph.gsa.GatherFunction}, {@link org.apache.flink.graph.gsa.SumFunction} as well as
 * {@link org.apache.flink.graph.gsa.ApplyFunction}.
 *
 * The GSAConfiguration object is passed as an argument to
 * {@link org.apache.flink.graph.Graph#runGatherSumApplyIteration(org.apache.flink.graph.gsa.GatherFunction,
 * org.apache.flink.graph.gsa.SumFunction, org.apache.flink.graph.gsa.ApplyFunction, int)}
 */
public class GSAConfiguration extends IterationConfiguration {

	/** the broadcast variables for the gather function **/
	private List<Tuple2<String, DataSet<?>>> bcVarsGather = new ArrayList<Tuple2<String,DataSet<?>>>();

	/** the broadcast variables for the sum function **/
	private List<Tuple2<String, DataSet<?>>> bcVarsSum = new ArrayList<Tuple2<String,DataSet<?>>>();

	/** the broadcast variables for the apply function **/
	private List<Tuple2<String, DataSet<?>>> bcVarsApply = new ArrayList<Tuple2<String,DataSet<?>>>();

	private boolean customPartition = false;

	public GSAConfiguration() {}

	/**
	 * Adds a data set as a broadcast set to the gather function.
	 *
	 * @param name The name under which the broadcast data is available in the gather function.
	 * @param data The data set to be broadcasted.
	 */
	public void addBroadcastSetForGatherFunction(String name, DataSet<?> data) {
		this.bcVarsGather.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Adds a data set as a broadcast set to the sum function.
	 *
	 * @param name The name under which the broadcast data is available in the sum function.
	 * @param data The data set to be broadcasted.
	 */
	public void addBroadcastSetForSumFunction(String name, DataSet<?> data) {
		this.bcVarsSum.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Adds a data set as a broadcast set to the apply function.
	 *
	 * @param name The name under which the broadcast data is available in the apply function.
	 * @param data The data set to be broadcasted.
	 */
	public void addBroadcastSetForApplyFunction(String name, DataSet<?> data) {
		this.bcVarsApply.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Get the broadcast variables of the GatherFunction.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast DataSet.
	 */
	public List<Tuple2<String, DataSet<?>>> getGatherBcastVars() {
		return this.bcVarsGather;
	}

	/**
	 * Get the broadcast variables of the SumFunction.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast DataSet.
	 */
	public List<Tuple2<String, DataSet<?>>> getSumBcastVars() {
		return this.bcVarsSum;
	}

	/**
	 * Get the broadcast variables of the ApplyFunction.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast DataSet.
	 */
	public List<Tuple2<String, DataSet<?>>> getApplyBcastVars() {
		return this.bcVarsApply;
	}

	public boolean isCustomPartition() {
		return customPartition;
	}

	public void setCustomPartition(boolean customPartition) {
		this.customPartition = customPartition;
	}
}

