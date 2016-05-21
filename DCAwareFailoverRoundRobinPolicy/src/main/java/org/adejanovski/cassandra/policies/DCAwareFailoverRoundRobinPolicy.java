/*
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.adejanovski.cassandra.policies;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.CloseableLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * A data-center aware Round-robin load balancing policy with DC failover
 * support.
 *
 * This policy provides round-robin queries over the node of the local data
 * center. It also includes in the query plans returned a configurable number of
 * hosts in the remote data centers, but those are always tried after the local
 * nodes. In other words, this policy guarantees that no host in a remote data
 * center will be queried unless no host in the local data center can be
 * reached.
 *
 * If used with a single data center, this policy is equivalent to the
 * RoundRobinPolicy, but its DC awareness incurs a
 * slight overhead so the RoundRobinPolicy could be
 * preferred to this policy in that case.
 *
 * On top of the DCAwareRoundRobinPolicy, this policy uses a one way switch in
 * case a defined number of nodes are down in the local DC. As stated, the
 * policy never switches back to the local DC in order to prevent
 * inconsistencies and give ops teams the ability to repair the local DC before
 * switching back manually.
 */
/**
 * @author adejanovski
 *
 */
/**
 * @author adejanovski
 *
 */
public class DCAwareFailoverRoundRobinPolicy implements LoadBalancingPolicy,
		CloseableLoadBalancingPolicy {

	private static final Logger logger = LoggerFactory
			.getLogger(DCAwareFailoverRoundRobinPolicy.class);

	/**
     * Returns a builder to create a new instance.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }
	
	private final String UNSET = "";

	private volatile Object lock = new Object();
	
	private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perDcLiveHosts = new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
	/**
	 * Map that holds the lost token ranges per keyspace.
	 * In order to use a simple map, the key is composed of [DC]-[Keyspace]-[Starting token of range]-[Ending token of range]
	 */
	private volatile ConcurrentMap<KeyspaceTokenRange, Integer> lostTokenRanges = new ConcurrentHashMap<KeyspaceTokenRange, Integer>();
	private volatile ConcurrentMap<KeyspaceTokenRange, ConsistencyLevel> maxAchievableConsistencyPerKeyspace = new ConcurrentHashMap<KeyspaceTokenRange, ConsistencyLevel>();
	private volatile ConsistencyLevel minAchievableConsistencyOverall;
	private final AtomicInteger index = new AtomicInteger();
	private final Map<ConsistencyLevel, Integer> consistencyLevelWeight = Maps.newHashMap();
	private List<FailoverSwitchCallback> callbacks = Lists.newArrayList();
	
	
	private Metadata clusterMetadata;

	volatile String localDc;
	volatile String backupDc;

	/**
	 * If the minimumRequiredConsistencyLevel is not achievable then switch to backup DC
	 */
	private ConsistencyLevel minimumRequiredConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
		
	/**
	 * flag to test if the switch as occurred
	 */
	private AtomicBoolean switchedToBackupDc = new AtomicBoolean(false);
	
	/**
	 * Time at which the switch occurred
	 */
	private Date switchedToBackupDcAt;

	/**
	 * Automatically switching back to local DC is possible after : downtime*{@code switchBackDelayFactor}
	 */
	private Float switchBackDelayFactor=(float)1.5;

	/**
	 * Downtime delay after which switching back cannot be automated (usually
	 * when hinted handoff window is reached) In seconds.
	 */
	private int noSwitchBackDowntimeDelay=0;
	
	/**
	 * Automatically switching back to local DC is possible after a minimum time
	 * to avoid constant switches in case of transient failures
	 * in seconds
	 */
	private Float minimumTimeBetweenSwitches=(float)300;

	private Date localDcCameBackUpAt;
	private boolean switchBackCanNeverHappen=false;

	private volatile Configuration configuration;

	/**
	 * Creates a new datacenter aware failover round robin policy that uses a
	 * local data-center and a backup data-center. Switching to the backup DC is
	 * triggered automatically if local DC loses more than
	 * {@code hostDownSwitchThreshold} nodes. Switching back to local DC after
	 * going to backup will never happen automatically.
	 * @param localDc the local datacenter
	 * @param backupDc the backup datacenter
	 * @param hostDownSwitchThreshold how many nodes have to be down before switching
	 * @throws InvalidConsistencyLevelException 
	 */
	public DCAwareFailoverRoundRobinPolicy(String localDc, String backupDc,	ConsistencyLevel minimumAchievableConsistencyLevel) throws InvalidConsistencyLevelException {

		this(localDc, backupDc, minimumAchievableConsistencyLevel, (float) -1.0, 0);

	}
	
	public DCAwareFailoverRoundRobinPolicy(String localDc, String backupDc,
			ConsistencyLevel minimumAchievableConsistencyLevel, float switchBackDelayFactor,
			int noSwitchBackDowntimeDelay) throws InvalidConsistencyLevelException {
		
		this(localDc, backupDc, minimumAchievableConsistencyLevel, switchBackDelayFactor, noSwitchBackDowntimeDelay, null);
		
	}

	/**
	 * Creates a new datacenter aware failover round robin policy that uses a
	 * local data-center and a backup data-center. Switching to the backup DC is
	 * triggered automatically if local DC cannot achieved the desired 
	 * {@code minimumAchievableConsistencyLevel} consistency.
	 * The policy will switch back to the local DC if conditions are fulfilled : 
	 * - Downtime lasted less than noSwitchBackDowntimeDelay (hint window)
	 * - uptime since downtime happened is superior to downtime*switchBackDelayFactor (give
	 * 	 enough time for hints to be executed)
	 * 
	 * @param localDc the local datacenter
	 * @param backupDc the backup datacenter
	 * @param minimumAchievableConsistencyLevel the lowest acceptable consistency level below which a switch will be triggered
	 * @param switchBackDelayFactor uptime since downtime happened is superior to downtime*switchBackDelayFactor
	 * @param noSwitchBackDowntimeDelay maximum downtime to authorize a back switch to local DC
	 * @param callback object that implements the FailoverSwitchCallback interface and gets called after a switch occurs (both ways)
	 * @throws InvalidConsistencyLevelException 
	 */
	public DCAwareFailoverRoundRobinPolicy(String localDc, String backupDc,
			ConsistencyLevel minimumAchievableConsistencyLevel, float switchBackDelayFactor,
			int noSwitchBackDowntimeDelay, FailoverSwitchCallback callback) throws InvalidConsistencyLevelException {
		
		if(minimumRequiredConsistencyLevel != ConsistencyLevel.LOCAL_ONE && minimumRequiredConsistencyLevel != ConsistencyLevel.LOCAL_QUORUM){
    		throw new InvalidConsistencyLevelException("Minimum required CL must be any of LOCAL_ONE or LOCAL_QUORUM. Please provide one of those two.");
    	}
		
		this.localDc = localDc == null ? UNSET : localDc;
		this.backupDc = backupDc == null ? UNSET : backupDc;
		this.minimumRequiredConsistencyLevel = minimumAchievableConsistencyLevel;		
		this.switchBackDelayFactor = switchBackDelayFactor;
		this.noSwitchBackDowntimeDelay = noSwitchBackDowntimeDelay;
		if(callback!=null){
			this.callbacks.add(callback);
		}
		
		
		consistencyLevelWeight.put(ConsistencyLevel.ANY, 0);
		consistencyLevelWeight.put(ConsistencyLevel.LOCAL_ONE, 1);
		consistencyLevelWeight.put(ConsistencyLevel.LOCAL_QUORUM, 5);
	}

	public void init(Cluster cluster, Collection<Host> hosts) {
		clusterMetadata = cluster.getMetadata();
		if (localDc != UNSET)
			logger.info(
					"Using provided data-center name '{}' for DCAwareFailoverRoundRobinPolicy",
					localDc);

		this.configuration = cluster.getConfiguration();

		ArrayList<String> notInLocalDC = new ArrayList<String>();

		for (Host host : hosts) {
			String dc = dc(host);
			

			logger.trace("node {} is in dc {}", host.getAddress().toString(), dc);
			// If the localDC was in "auto-discover" mode and it's the first
			// host for which we have a DC, use it.
			if (localDc == UNSET && dc != UNSET) {
				logger.info(
						"Using data-center name '{}' for DCAwareFailoverRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareFailoverRoundRobinPolicy constructor)",
						dc);
				localDc = dc;
			} else if (!dc.equals(localDc) && !dc.equals(backupDc))
				notInLocalDC.add(String.format("%s (%s)", host.toString(), dc));

			if (!dc.equals(localDc) && !dc.equals(backupDc))
				notInLocalDC.add(String.format("%s (%s)", host.toString(),
						host.getDatacenter()));

			CopyOnWriteArrayList<Host> prev = perDcLiveHosts.get(dc);
			if (prev == null)
				perDcLiveHosts.put(dc, new CopyOnWriteArrayList<Host>(
						Collections.singletonList(host)));
			else
				prev.addIfAbsent(host);
		}

		if (notInLocalDC.size() > 0) {
			String nonLocalHosts = Joiner.on(",").join(notInLocalDC);
			logger.warn(
					"Some contact points don't match local or backup data center. Local DC = {} - backup DC {}. Non-conforming contact points: {}",
					localDc, backupDc, nonLocalHosts);
		}
	}

	private String dc(Host host) {
		String dc = host.getDatacenter();
		return dc == null ? localDc : dc;
	}

	@SuppressWarnings("unchecked")
	private static CopyOnWriteArrayList<Host> cloneList(
			CopyOnWriteArrayList<Host> list) {
		return (CopyOnWriteArrayList<Host>) list.clone();
	}

	/**
	 * Return the HostDistance for the provided host.
	 * 
	 * This policy consider nodes in the local datacenter as {@code LOCAL}. For
	 * each remote datacenter, it considers a configurable number of hosts as
	 * {@code REMOTE} and the rest is {@code IGNORED}.
	 * 
	 * To configure how many host in each remote datacenter is considered
	 * {@code REMOTE}.
	 *
	 * @param host
	 *            the host of which to return the distance of.
	 * @return the HostDistance to {@code host}.
	 */
	public HostDistance distance(Host host) {
		String dc = dc(host);
		// If the connection has switched to the backup DC and fulfills
		// the requirement for a back switch, make it happen.
		if(!switchBackCanNeverHappen){
			triggerBackSwitchIfNecessary();
		}

		if (isLocal(dc)) {			
			return HostDistance.LOCAL;
		}

		// Only hosts in local DC and backup DC can be considered remote
		if(dc(host).equals(localDc) || dc(host).equals(backupDc))
			return HostDistance.REMOTE;
		
		// All other hosts are ignored
		return HostDistance.IGNORED;
		
	}

	/**
	 * Returns the hosts to use for a new query.
	 * 
	 * The returned plan will always try each known host in the local datacenter
	 * first, and then, if none of the local host is reachable, will try up to a
	 * configurable number of other host per remote datacenter. The order of the
	 * local node in the returned query plan will follow a Round-robin
	 * algorithm.
	 *
	 * @param loggedKeyspace
	 *            the keyspace currently logged in on for this query.
	 * @param statement
	 *            the query for which to build the plan.
	 * @return a new query plan, i.e. an iterator indicating which host to try
	 *         first for querying, which one to use as failover, etc...
	 */
	public Iterator<Host> newQueryPlan(String loggedKeyspace,
			final Statement statement) {
		String currentDc = localDc;
		if(!switchBackCanNeverHappen){
			triggerBackSwitchIfNecessary();
		}

		if (switchedToBackupDc.get()) {
			currentDc = backupDc;
		}
		
		CopyOnWriteArrayList<Host> localLiveHosts = perDcLiveHosts.get(currentDc);
		final List<Host> hosts = localLiveHosts == null ? Collections.<Host> emptyList() : cloneList(localLiveHosts);

		final int startIdx = index.getAndIncrement();

		return new AbstractIterator<Host>() {

			private int idx = startIdx;
			private int remainingLocal = hosts.size();

			// For remote Dcs
			private Iterator<String> remoteDcs;
			private List<Host> currentDcHosts;
			private int currentDcRemaining;

			@Override
			protected Host computeNext() {				
				if (remainingLocal > 0) {
					remainingLocal--;					
					int c = idx++ % hosts.size();
					if (c < 0) {
						c += hosts.size();
					}					
					return hosts.get(c);
				}
									
				return endOfData();
			}
		};
	}

	public void onUp(Host host) {
		updateLostTokensOnNodeUp(host);
		String dc = dc(host);		
		if (dc.equals(localDc) // && computeMaxTokenReplicasLost() >= this.tokenReplicaLostSwitchThreshold
				) {
			// if a node comes backup in the local DC and we're not already
			// equal to the initial threshold, add one node to the
			// switch threshold
			// This can only happen if the switch didn't occur yet			
			updateLocalDcStatus();
		}
		// If the localDC was in "auto-discover" mode and it's the first host
		// for which we have a DC, use it.
		if (localDc == UNSET && dc != UNSET) {
			logger.info(
					"Using data-center name '{}' for DCAwareFailoverRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareFailoverRoundRobinPolicy constructor)",
					dc);
			localDc = dc;
		}

		CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
		if (dcHosts == null) {
			CopyOnWriteArrayList<Host> newMap = new CopyOnWriteArrayList<Host>(Collections.singletonList(host));
			dcHosts = perDcLiveHosts.putIfAbsent(dc, newMap);
			// If we've successfully put our new host, we're good, otherwise
			// we've been beaten so continue
			if (dcHosts == null)
				return;
		}
		dcHosts.addIfAbsent(host);
	}

	public void onSuspected(Host host) {
	}

	public void onDown(Host host) {		
		updateLostTokensOnNodeDown(host);
		
		CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc(host));
		if (dcHosts != null)
			dcHosts.remove(host);

		if (!canFulfillMinimumCl()) {
			// Local DC can't keep up with CL
			// Make sure localDc is not considered as being up
			localDcCameBackUpAt = null;
			if (!switchedToBackupDc.get()) {
				// if we lost as many nodes in the local dc as configured in the
				// threshold, switch to backup DC
				switchToBackup();
			}
		}
	}

	public void onAdd(Host host) {
		onUp(host);
	}

	public void onRemove(Host host) {
		onDown(host);
	}

	public void close() {
		// nothing to do
	}

	/**
	 * Perform switch to backup DC
	 */
	private void switchToBackup() {
		switchedToBackupDc.set(true);
		switchedToBackupDcAt = new Date();
		logger.warn(
				"Lost ability to fulfill {} for some tokens in data-center '{}'. Switching to data-center '{}'",
				minimumRequiredConsistencyLevel.name(), this.localDc, this.backupDc);
		for(FailoverSwitchCallback callback:callbacks){
			try{
				callback.switchDcCallback(backupDc, maxAchievableConsistencyPerKeyspace, lostTokenRanges, this.localDc, this.backupDc);
			}catch(Exception e){
				logger.warn("Execution of callback " + callback.getCallbackName() + "failed.", e );
			}
		}

	}

	/**
	 * Perform switch back to local DC
	 */
	private void switchBackToLocal() {
		switchedToBackupDc.set(false);
		switchedToBackupDcAt = null;
		localDcCameBackUpAt = null;
		logger.warn(
				"Recovered enough nodes in data-center '{}'. Switching back since conditions are fulfilled",
				this.localDc);

	}

	/**
	 * Check if the cluster state fulfills requirements for switching back to
	 * local DC. Conditions to switch back : - the connection as already
	 * switched to backup DC - hostDownSwitchThreshold is > 0 - Enough time has
	 * passed for hinted handoff (currentTime - localDcCameBackUpAt) >
	 * (localDcCameBackUpAt - switchedToBackupDcAt)*switchBackDelayFactor -
	 * (localDcCameBackUpAt - switchedToBackupDcAt) < noSwitchBackDowntimeDelay
	 * 
	 * @return
	 */
	private boolean canSwitchBack() {		
		if ((localDcCameBackUpAt.getTime() - switchedToBackupDcAt.getTime()) < noSwitchBackDowntimeDelay * 1000) {
			if (switchedToBackupDc.get() && isLocalDcBackUp()) {
				logger.debug(
						"Local DC {} is up and has been down for {}s. Switch back will happen after {}s. Uptime = {}s ",
						localDc,
						(int) (getDowntimeDuration() / 1000),
						(int) (getDowntimeDuration() * switchBackDelayFactor / 1000),
						(getUptimeDuration()) / 1000);
				
				return (canFulfillMinimumCl())
						&& (getUptimeDuration() > getDowntimeDuration() * switchBackDelayFactor)
						&& getDowntimeDuration() < noSwitchBackDowntimeDelay * 1000;
			}
		}else{
			// Downtime lasted more than the hinted handoff window
			// Switching back is now a manual operation
			logger.warn(
					"Local DC has been down for too long. Switch back will never happen.");
			switchBackCanNeverHappen=true;
		}

		return false;

	}
	
	/**
	 * returns the duration of the local DC downtime.
	 * @return
	 */
	private long getDowntimeDuration(){
		return localDcCameBackUpAt.getTime() - switchedToBackupDcAt.getTime();
	}
	
	/**
	 * get the uptime duration of local DC after outage.
	 * @return
	 */
	private long getUptimeDuration(){		
		return new Date().getTime() - localDcCameBackUpAt.getTime();
	}
	
	

	private void updateLocalDcStatus() {		
		if (switchedToBackupDc.get() && canFulfillMinimumCl() && localDcCameBackUpAt == null) {
			localDcCameBackUpAt = new Date();
			logger.info("local DC just came back up");
		}
	}

	/**
	 * Test if local DC has enough nodes to be considered alive
	 * 
	 * @return
	 */
	private boolean isLocalDcBackUp() {		
		return canFulfillMinimumCl() && localDcCameBackUpAt != null;
	}

	/**
	 * Test if a node is in the local DC (or in the backup DC and switch has
	 * occurred)
	 * 
	 * @param dc
	 * @return
	 */
	private boolean isLocal(String dc) {
		return dc == UNSET || (dc.equals(localDc) && !switchedToBackupDc.get())
				|| (dc.equals(backupDc) && switchedToBackupDc.get());
	}

	/**
	 * Check if a switch as occurred and switching back to local DC is possible.
	 */
	public void triggerBackSwitchIfNecessary() {
		if (switchedToBackupDc.get() && localDcCameBackUpAt!=null && switchedToBackupDcAt!=null) {
			if (canSwitchBack()) {
				switchBackToLocal();
			}
		}
	}
	
	
	/**
     * Update the map of lost tokens when a node goes down.
     * 
     * @param host     
     * @return the max number of replicas lost for a token range
     */
    public ConsistencyLevel updateLostTokensOnNodeDown(Host host){
    	synchronized (lock) {
					
	    	logger.info("host {} is down and has those tokens {}", host.getAddress(), new TreeSet<Token>(host.getTokens()));    	
	    	
	    	for(KeyspaceMetadata keyspace:clusterMetadata.getKeyspaces()){
	    		if(!keyspace.getName().startsWith("system")){
		    		int maxLost = 0;
		    		Set<TokenRange> rangesForKeyspace = clusterMetadata.getTokenRanges(keyspace.getName(), host);    				    			    	
		    		for(TokenRange tokenRange:rangesForKeyspace){
		    			KeyspaceTokenRange ksTokenRange = new KeyspaceTokenRange(host.getDatacenter(), keyspace.getName(), tokenRange.getStart().toString(), tokenRange.getEnd().toString());
		    			lostTokenRanges.putIfAbsent(ksTokenRange, 0);
		    			int newValue = lostTokenRanges.get(ksTokenRange);
		    			newValue++;
		    			lostTokenRanges.put(ksTokenRange, newValue);
		    			maxLost = Math.max(maxLost, newValue);    					    			
		    		}
		    		
		    		// if keyspace is using NTS and has replicas on local DC
		    		logger.info("Keyspace {} replication = {}", keyspace.getName(), keyspace.getReplication());		    		
		    		if(keyspace.getReplication().get("class").toLowerCase().contains("networktopologystrategy") && keyspace.getReplication().containsKey(localDc)){
		    			
		    			int nbReplicas = Integer.parseInt(keyspace.getReplication().get(localDc));
		    			logger.info("Keyspace {} maxLost = {} nbReplicas = {}", keyspace.getName(), maxLost, nbReplicas);
		    			ConsistencyLevel maxClForKeyspace = ConsistencyLevel.LOCAL_QUORUM;
		    			int neededNodesForQuorum = (int)((float)nbReplicas/(float)2)+1;
		    			int nodesLeft = nbReplicas-maxLost;
		    			logger.info("Keyspace {} needed for quorum = {} nodes left = {}", keyspace.getName(), neededNodesForQuorum, nodesLeft);
		    			if(nodesLeft == 0){
		    				maxClForKeyspace = ConsistencyLevel.ANY;
		    			}else if(nodesLeft < neededNodesForQuorum){
		    				maxClForKeyspace = ConsistencyLevel.LOCAL_ONE;
		    			}
		    			maxAchievableConsistencyPerKeyspace.put(new KeyspaceTokenRange(host.getDatacenter(), keyspace.getName()), maxClForKeyspace);
		    		}else{
		    			logger.info("Oooops, pb de parsing des infos du keyspace");
		    		}
	    		}
	    		    		
	    	}
	    	
	    	
	    	return computeMinAchievableClOverall();
    	}
    }    
    
    /**
     * Update the map of lost tokens when a node comes up.
     * 
     * @param host
     */
    public ConsistencyLevel updateLostTokensOnNodeUp(Host host){
    	synchronized (lock) {
	    	logger.info(host.toString() + " is up");
	    	for(KeyspaceMetadata keyspace:clusterMetadata.getKeyspaces()){
	    		if(!keyspace.getName().startsWith("system")){
	    			int maxLost = 0;
		    		Set<TokenRange> rangesForKeyspace = clusterMetadata.getTokenRanges(keyspace.getName(), host);		    				    	
		    		for(TokenRange tokenRange:rangesForKeyspace){
		    			KeyspaceTokenRange ksTokenRange = new KeyspaceTokenRange(host.getDatacenter(), keyspace.getName(), tokenRange.getStart().toString(), tokenRange.getEnd().toString());
		    			Integer added = lostTokenRanges.putIfAbsent(ksTokenRange, 0);
		    			
		    			// the map already existed so we need to decrement the value if it is superior to 0
		    			int newValue = lostTokenRanges.get(ksTokenRange);
		    			if(newValue>0)
		    				newValue--;
		    			
		    			maxLost = Math.max(maxLost, newValue);    					    			
		    			lostTokenRanges.put(ksTokenRange, newValue);		    			
		    		}
		    		
		    		// if keyspace is using NTS and has replicas on local DC
		    		if(keyspace.getReplication().get("class").toLowerCase().contains("networktopologystrategy") && keyspace.getReplication().containsKey(localDc)){
		    			int nbReplicas = Integer.parseInt(keyspace.getReplication().get(localDc));
		    			ConsistencyLevel maxClForKeyspace = ConsistencyLevel.LOCAL_QUORUM;		    			
		    			int neededNodesForQuorum = (int)((float)nbReplicas/(float)2)+1;
		    			int nodesLeft = nbReplicas-maxLost;
		    			logger.info("Keyspace {} needed for quorum = {} nodes left = {}", keyspace.getName(), neededNodesForQuorum, nodesLeft);
		    			if(nodesLeft == 0){
		    				maxClForKeyspace = ConsistencyLevel.ANY;
		    			}else if(nodesLeft < neededNodesForQuorum){
		    				maxClForKeyspace = ConsistencyLevel.LOCAL_ONE;
		    			}
		    			maxAchievableConsistencyPerKeyspace.put(new KeyspaceTokenRange(host.getDatacenter(), keyspace.getName()), maxClForKeyspace);
		    		}
	    		}
	    		    		
	    	}
	    	
	    	return computeMinAchievableClOverall();
    	}
    }
    
    
    /**
     * Update the map of lost tokens when a node comes up.
     * 
     * @param host
     */
    public ConsistencyLevel computeMinAchievableClOverall(){
    	ConsistencyLevel minCl = ConsistencyLevel.LOCAL_QUORUM;
		for(Entry<KeyspaceTokenRange, ConsistencyLevel> keyspaceTokenRangeMaxCl:maxAchievableConsistencyPerKeyspace.entrySet()){			
			if(consistencyLevelWeight.get(keyspaceTokenRangeMaxCl.getValue()) < consistencyLevelWeight.get(minCl)){
				minCl = keyspaceTokenRangeMaxCl.getValue();
			}
		}
		
		this.minAchievableConsistencyOverall = minCl;
		logger.info("Min achievable CL overall is " + minCl.name());
		return minCl;
    }
    
    private Boolean canFulfillMinimumCl(){
    	return consistencyLevelWeight.get(minAchievableConsistencyOverall) >= consistencyLevelWeight.get(minimumRequiredConsistencyLevel);
    }
    
    
	
	
	/**
     * Helper class to build the policy.
     */
    public static class Builder {
        private String localDc;
        private String backupDc;        
        private ConsistencyLevel minimumRequiredConsistencyLevel;
        private Float switchBackDelayFactor=(float)1000;
    	private int noSwitchBackDowntimeDelay=0;
    	private List<FailoverSwitchCallback> callbacks = Lists.newArrayList();

        /**
         * Sets the name of the datacenter that will be considered "local" by the policy.
         * 
         * This must be the name as known by Cassandra (in other words, the name in that appears in
         * {@code system.peers}, or in the output of admin tools like nodetool).
         * 
         * If this method isn't called, the policy will default to the datacenter of the first node
         * connected to. This will always be ok if all the contact points use at {@code Cluster}
         * creation are in the local data-center. Otherwise, you should provide the name yourself
         * with this method.
         *
         * @param localDc the name of the datacenter. It should not be {@code null}.
         * @return this builder.
         */
        public Builder withLocalDc(String localDc) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(localDc),
                "localDc name can't be null or empty. If you want to let the policy autodetect the datacenter, don't call Builder.withLocalDC");
            this.localDc = localDc;
            return this;
        }
        
        /**
         * Sets the name of the datacenter that will be considered as "backup" by the policy.
         * <p>
         * This must be the name as known by Cassandra (in other words, the name in that appears in
         * {@code system.peers}, or in the output of admin tools like nodetool).
         * <p>
         * If this method must be called, otherwise you should not use this policy.
         *
         * @param backupDc the name of the datacenter. It should not be {@code null}.
         * @return this builder.
         */
        public Builder withBackupDc(String backupDc) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(localDc),
                "backupDc name can't be null or empty.");
            this.backupDc = backupDc;
            return this;
        }

        
        /**
         * Sets how many replicas must be lost for a token range in the local DC before switching to backup.  
         * 
         * @param tokenReplicaLostSwitchThreshold the number of nodes down before switching to the backup DC.
         * @return this builder
         * @throws Exception 
         */
        public Builder withMinimumRequiredConsistencyLevel(ConsistencyLevel minimumRequiredConsistencyLevel) throws Exception {        	
            this.minimumRequiredConsistencyLevel = minimumRequiredConsistencyLevel;
            return this;
        }
        
        /**
         * Mandatory if you want to authorize switching back to local DC after downtime. 
         * Allows enough time to pass so that hinted handoff can finish 
         * (currentTime - localDcCameBackUpAt) &gt; (localDcCameBackUpAt - switchedToBackupDcAt)*switchBackDelayFactor 
         * 
         * @param switchBackDelayFactor times downtime has to be &lt;= uptime before switching back to local DC 
         * @return this builder
         */
        public Builder withSwitchBackDelayFactor(float switchBackDelayFactor) {
            this.switchBackDelayFactor = switchBackDelayFactor;
            return this;
        }
        
        /**
         * Mandatory if you want to authorize switching back to local DC after downtime.
         * Prevents switching back to local DC if downtime was longer than the provided value.
         * Used to check if downtime didn't last more than the hinted handoff window (which requires repair).
         * 
         * @param noSwitchBackDowntimeDelay max time in seconds before switching back to local DC will be prevented.
         * @return this builder
         */
        public Builder withNoSwitchBackDowntimeDelay(int noSwitchBackDowntimeDelay) {
            this.noSwitchBackDowntimeDelay = noSwitchBackDowntimeDelay;
            return this;
        }
        
        public Builder withFailoverSwitchCallback(FailoverSwitchCallback callback) {
            this.callbacks.add(callback);
            return this;
        }
        
        

        /**
         * Builds the policy configured by this builder.
         *
         * @return the policy.
         * @throws InvalidConsistencyLevelException 
         */
        public DCAwareFailoverRoundRobinPolicy build() throws InvalidConsistencyLevelException {
            return new DCAwareFailoverRoundRobinPolicy(localDc, backupDc, minimumRequiredConsistencyLevel, switchBackDelayFactor, noSwitchBackDowntimeDelay);
        }
        
        
        
    }
    
    public class KeyspaceTokenRange{
    	String datacenter;
    	String keyspace;
    	String startToken;
    	String endToken;
    	
    	public KeyspaceTokenRange(String datacenter, String keyspace, String startToken, String endToken){
    		this.datacenter = datacenter;
    		this.keyspace = keyspace;
    		this.startToken = startToken;
    		this.endToken = endToken;    		
    	}
    	
    	public KeyspaceTokenRange(String datacenter, String keyspace){
    		this(datacenter, keyspace, "", "");    		
    	}

    	public String getDatacenter() {
			return datacenter;
		}

		public void setDatacenter(String datacenter) {
			this.datacenter = datacenter;
		}

		public String getKeyspace() {
			return keyspace;
		}

		public void setKeyspace(String keyspace) {
			this.keyspace = keyspace;
		}

		public String getStartToken() {
			return startToken;
		}

		public void setStartToken(String startToken) {
			this.startToken = startToken;
		}

		public String getEndToken() {
			return endToken;
		}

		public void setEndToken(String endToken) {
			this.endToken = endToken;
		}

		@Override
        public boolean equals(Object obj) {
            if (obj == null) return false;

            if( ! (obj instanceof KeyspaceTokenRange) ) return false;

            KeyspaceTokenRange other = (KeyspaceTokenRange) obj;

            return this.datacenter.equals(other.getDatacenter())
            		&& this.keyspace.equals(other.getKeyspace())
            		&& this.startToken.equals(other.getStartToken())
            		&& this.endToken.equals(other.getEndToken());
        }

		public int hashCode() {
			return (datacenter.toString() + "-" + this.keyspace + "-" + this.startToken + "-" + this.endToken).hashCode() ;
		}

		public String toString() {
			return datacenter.toString() + "-" + this.keyspace + "-" + this.startToken + "-" + this.endToken;
		}    	
    	
    }
    
    
    
    public class InvalidConsistencyLevelException extends Exception {

        /**
		 * 
		 */
		private static final long serialVersionUID = -4596556548649384959L;

		public InvalidConsistencyLevelException(String message) {
            super(message);
        }

        public InvalidConsistencyLevelException(String message, Throwable throwable) {
            super(message, throwable);
        }

    }

}
