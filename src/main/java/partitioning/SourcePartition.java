package partitioning;

import org.apache.flink.api.common.functions.Partitioner;

public class SourcePartition implements Partitioner<String> {
	
	@Override
	public int partition(String source, int numPartitions) {
		return (int) Long.parseLong(source) % numPartitions;
	}
}
