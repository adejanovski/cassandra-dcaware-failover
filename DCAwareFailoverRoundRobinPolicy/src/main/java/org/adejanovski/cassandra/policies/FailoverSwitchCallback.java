package org.adejanovski.cassandra.policies;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy.KeyspaceTokenRange;

import com.datastax.driver.core.ConsistencyLevel;

public interface FailoverSwitchCallback {
	
	/**
	 * returns the callback name to help distinguish callbacks.
	 * 
	 * @return
	 */
	public String getCallbackName();
	
	/**
	 * Method that gets called each time a switch occurs between DCs (either way). 
	 * 
	 * @param currentDc
	 * @param maxAchievableConsistencyPerKeyspace
	 * @param lostTokenRanges
	 * @param localDc
	 * @param backupDc
	 * @param localDcCameBackUpAt 
	 * @param switchedToBackupDcAt 
	 * @param switchBackCanNeverHappen 
	 */
	public void switchDcCallback(String currentDc, ConcurrentMap<KeyspaceTokenRange, ConsistencyLevel> maxAchievableConsistencyPerKeyspace, ConcurrentMap<KeyspaceTokenRange, Set<String>> lostTokenRanges, String localDc, String backupDc, Date switchedToBackupDcAt);
	

}
