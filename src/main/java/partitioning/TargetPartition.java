package partitioning;

import org.apache.flink.api.common.functions.Partitioner;

public class TargetPartition implements Partitioner<String> {

	@Override
	public int partition(String target, int numpartitions) {
		return (int) Long.parseLong(target) % numpartitions;
	}
}
