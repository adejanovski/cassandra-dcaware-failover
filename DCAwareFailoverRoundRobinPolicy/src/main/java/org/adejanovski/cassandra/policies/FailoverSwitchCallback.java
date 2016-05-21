package org.adejanovski.cassandra.policies;

import java.util.concurrent.ConcurrentMap;
import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy.KeyspaceTokenRange;

import com.datastax.driver.core.ConsistencyLevel;

public interface FailoverSwitchCallback {
	
	public String getCallbackName();
	public void switchDcCallback(String currentDc, ConcurrentMap<KeyspaceTokenRange, ConsistencyLevel> maxAchievableConsistencyPerKeyspace, ConcurrentMap<KeyspaceTokenRange, Integer> lostTokenRanges, String localDc, String backupDc);

}
