package hadoop.practice.book.chapter4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class ComputeIntensiveSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {

	/**
	 * For every existing server, try to assign a split with data local to that
	 * server. Assign the remaining splits randomly to the next servers.
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> originalSplits = super.getSplits(job);
		String[] activeServers = getActiveServers(job);
		if (activeServers == null)
			return null;
		int numActiveServers = activeServers.length;
		int numSplits = originalSplits.size();
		List<InputSplit> newSplits = new ArrayList<InputSplit>(numSplits);
		int currentServerIndex = 0;

		// For each of the new splits needed
		for (int i = 0; i < numSplits; i++) {
			// Get the next server
			String server = activeServers[currentServerIndex];
			boolean replaced = false;

			// Look through all the original splits
			for (InputSplit split : originalSplits) {
				FileSplit fs = (FileSplit) split;
				// For each location in the original split
				for (String l : fs.getLocations()) {
					// If the current server is the same as the location of the
					// original split
					if (l.equals(server)) {
						newSplits.add(new FileSplit(fs.getPath(), fs.getStart(), fs.getLength(), new String[] { server }));
						originalSplits.remove(split);
						replaced = true;
						break;
					}
				}
				if (replaced)
					break;
			}
			if (!replaced) {
				FileSplit fs = (FileSplit) newSplits.get(0);
				newSplits.add(new FileSplit(fs.getPath(), fs.getStart(), fs.getLength(), new String[] { server }));
				originalSplits.remove(0);
			}
			int maxServerIndex = numActiveServers - 1;
			currentServerIndex = getTheNextServerIndex(currentServerIndex, maxServerIndex);
		}

		return newSplits;
	}

	private String[] getActiveServers(JobContext context) {
		String[] servers = null;
		try {
			JobClient jc = new JobClient((JobConf) context.getConfiguration());
			ClusterStatus status = jc.getClusterStatus(true);
			Collection<String> atc = status.getActiveTrackerNames();
			servers = new String[atc.size()];
			int s = 0;
			for (String serverInfo : atc) {
				StringTokenizer st = new StringTokenizer(serverInfo, ":");
				String trackerName = st.nextToken();
				StringTokenizer st1 = new StringTokenizer(trackerName, "_");
				st1.nextToken();
				servers[s++] = st1.nextToken();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return servers;
	}

	private static int getTheNextServerIndex(int current, int maxServerIndex) {
		boolean nextIsOverMax = false;
		if (current >= maxServerIndex) {
			nextIsOverMax = true;
		} else {
			nextIsOverMax = false;
		}
		return nextIsOverMax ? 0 : current + 1;
	}

}
