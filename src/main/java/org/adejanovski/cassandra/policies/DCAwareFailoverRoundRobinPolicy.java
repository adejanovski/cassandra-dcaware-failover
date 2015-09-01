/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.CloseableLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * A data-center aware Round-robin load balancing policy with DC failover support.
 * <p>
 * This policy provides round-robin queries over the node of the local data
 * center. It also includes in the query plans returned a configurable number of
 * hosts in the remote data centers, but those are always tried after the local
 * nodes. In other words, this policy guarantees that no host in a remote data
 * center will be queried unless no host in the local data center can be
 * reached.
 * <p>
 * If used with a single data center, this policy is equivalent to the
 * {@code LoadBalancingPolicy.RoundRobin} policy, but its DC awareness incurs a
 * slight overhead so the {@code LoadBalancingPolicy.RoundRobin} policy could be
 * preferred to this policy in that case.
 * <p>
 * On top of the DCAwareRoundRobinPolicy, this policy uses a one way switch in
 * case a defined number of nodes are down in the local DC.
 * As stated, the policy never switches back to the local DC in order to prevent
 * inconsistencies and give ops teams the ability to repair the local DC before
 * switching back manually.
 */
public class DCAwareFailoverRoundRobinPolicy implements LoadBalancingPolicy,
		CloseableLoadBalancingPolicy {

	private static final Logger logger = LoggerFactory
			.getLogger(DCAwareFailoverRoundRobinPolicy.class);

	private final String UNSET = "";

	private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perDcLiveHosts = new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
	private final AtomicInteger index = new AtomicInteger();

	@VisibleForTesting
	volatile String localDc;
	@VisibleForTesting
	volatile String backupDc;

	/**
	 * Current value of the switch threshold. if {@code hostDownSwitchThreshold} <= 0 then we must switch.
	 */
	private int hostDownSwitchThreshold;
	
	/**
	 * Initial value of the switch threshold
	 */
	private final int initHostDownSwitchThreshold;
	
	/**
	 * flag to test if the switch as occurred
	 */
	private boolean switchedToBackupDc = false;
	
	/**
	 * Time at which the switch occurred 
	 */
	private Date switchedToBackupDcAt; 	
	
	/**
	 * Automatically switching back to local DC is possible after : downtime*{@code switchBackDelayFactor} 
	 */
	private Float switchBackDelayFactor;
	
	/**
	 * Downtime delay after which switching back cannot be automated (usually when hinted handoff window is reached)
	 * In seconds.  
	 */
	private int noSwitchBackDowntimeDelay;

	private final int usedHostsPerRemoteDc;
	private final boolean dontHopForLocalCL;
	
	
	private volatile Configuration configuration;

	/**
	 * Creates a new datacenter aware failover round robin policy that uses a
	 * local data-center and a backup data-center.
	 * Switching to the backup DC is triggered automatically if local DC loses
	 * more than {@code hostDownSwitchThreshold} nodes.
	 * Switching back to local DC after going to backup will never happen automatically.
	 * <p> 
	 */

	public DCAwareFailoverRoundRobinPolicy(String localDc, String backupDc,
			int hostDownSwitchThreshold) {
		
		this(localDc, backupDc, hostDownSwitchThreshold, (float) -1.0, 0);
		
	}
	
	/**
	 * Creates a new datacenter aware failover round robin policy that uses a
	 * local data-center and a backup data-center.
	 * Switching to the backup DC is triggered automatically if local DC loses
	 * more than {@code hostDownSwitchThreshold} nodes.
	 * <p> 
	 */

	public DCAwareFailoverRoundRobinPolicy(String localDc, String backupDc,
			int hostDownSwitchThreshold, float switchBackDelayFactor, int noSwitchBackDowntimeDelay) {
		this.localDc = localDc == null ? UNSET : localDc;
		this.backupDc = backupDc == null ? UNSET : backupDc;
		this.usedHostsPerRemoteDc = 0;
		this.dontHopForLocalCL = true;
		this.hostDownSwitchThreshold = hostDownSwitchThreshold;
		this.initHostDownSwitchThreshold = hostDownSwitchThreshold;
		this.switchBackDelayFactor = switchBackDelayFactor;
		this.noSwitchBackDowntimeDelay = noSwitchBackDowntimeDelay;
		
	}


	public void init(Cluster cluster, Collection<Host> hosts) {
		if (localDc != UNSET)
			logger.info(
					"Using provided data-center name '{}' for DCAwareFailoverRoundRobinPolicy",
					localDc);

		this.configuration = cluster.getConfiguration();

		ArrayList<String> notInLocalDC = new ArrayList<String>();

		for (Host host : hosts) {
			String dc = dc(host);

			logger.info("node {} is in dc {}", host.getAddress().toString(), dc);
			// If the localDC was in "auto-discover" mode and it's the first
			// host for which we have a DC, use it.
			if (localDc == UNSET && dc != UNSET) {
				logger.info(
						"Using data-center name '{}' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)",
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
	 * <p>
	 * This policy consider nodes in the local datacenter as {@code LOCAL}. For
	 * each remote datacenter, it considers a configurable number of hosts as
	 * {@code REMOTE} and the rest is {@code IGNORED}.
	 * <p>
	 * To configure how many host in each remote datacenter is considered
	 * {@code REMOTE}, see {@link #DCAwareRoundRobinPolicy(String, int)}.
	 *
	 * @param host
	 *            the host of which to return the distance of.
	 * @return the HostDistance to {@code host}.
	 */
	public HostDistance distance(Host host) {
		String dc = dc(host);
		if (dc == UNSET || (dc.equals(localDc) && !switchedToBackupDc)
				|| (dc.equals(backupDc) && switchedToBackupDc)) {
			logger.debug("node {} is in LOCAL", host.getAddress().toString());
			return HostDistance.LOCAL;
		}

		logger.debug("node {} is REMOTE", host.getAddress().toString());

		CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
		if (dcHosts == null || usedHostsPerRemoteDc == 0)
			return HostDistance.IGNORED;

		// We need to clone, otherwise our subList call is not thread safe
		dcHosts = cloneList(dcHosts);
		return dcHosts.subList(0,
				Math.min(dcHosts.size(), usedHostsPerRemoteDc)).contains(host) ? HostDistance.REMOTE
				: HostDistance.IGNORED;
	}

	/**
	 * Returns the hosts to use for a new query.
	 * <p>
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

		if (switchedToBackupDc) {
			currentDc = backupDc;
		}

		CopyOnWriteArrayList<Host> localLiveHosts = perDcLiveHosts
				.get(currentDc);
		final List<Host> hosts = localLiveHosts == null ? Collections
				.<Host> emptyList() : cloneList(localLiveHosts);
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
				while (true) {
					if (remainingLocal > 0) {
						remainingLocal--;
						int c = idx++ % hosts.size();
						if (c < 0) {
							c += hosts.size();
						}
						return hosts.get(c);
					}

					if (currentDcHosts != null && currentDcRemaining > 0) {
						currentDcRemaining--;
						int c = idx++ % currentDcHosts.size();
						if (c < 0) {
							c += currentDcHosts.size();
						}
						return currentDcHosts.get(c);
					}

					ConsistencyLevel cl = statement.getConsistencyLevel() == null ? configuration
							.getQueryOptions().getConsistencyLevel()
							: statement.getConsistencyLevel();

					if (dontHopForLocalCL && cl.isDCLocal())
						return endOfData();

					if (remoteDcs == null) {
						Set<String> copy = new HashSet<String>(
								perDcLiveHosts.keySet());
						copy.remove(localDc);
						remoteDcs = copy.iterator();
					}

					if (!remoteDcs.hasNext())
						break;

					String nextRemoteDc = remoteDcs.next();
					CopyOnWriteArrayList<Host> nextDcHosts = perDcLiveHosts
							.get(nextRemoteDc);
					if (nextDcHosts != null) {
						// Clone for thread safety
						List<Host> dcHosts = cloneList(nextDcHosts);
						currentDcHosts = dcHosts.subList(0,
								Math.min(dcHosts.size(), usedHostsPerRemoteDc));
						currentDcRemaining = currentDcHosts.size();
					}
				}
				return endOfData();
			}
		};
	}

	public void onUp(Host host) {

		String dc = dc(host);

		if (dc.equals(localDc)
				&& this.hostDownSwitchThreshold < this.initHostDownSwitchThreshold
				) {
			// if a node comes backup in the local DC and we're not already
			// equal to the initial threshold, add one node to the one way
			// switch threshold
			// This can only happen if the switch didn't occur yet
			this.hostDownSwitchThreshold++;
		}
		// If the localDC was in "auto-discover" mode and it's the first host
		// for which we have a DC, use it.
		if (localDc == UNSET && dc != UNSET) {
			logger.info(
					"Using data-center name '{}' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareFailoverRoundRobinPolicy constructor)",
					dc);
			localDc = dc;
		}

		CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
		if (dcHosts == null) {
			CopyOnWriteArrayList<Host> newMap = new CopyOnWriteArrayList<Host>(
					Collections.singletonList(host));
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
		if (dc(host).equals(localDc) && !switchedToBackupDc) {
			// if a node goes down in the local DC remove one node to eventually
			// trigger the one way switch
			this.hostDownSwitchThreshold--;
		}
		CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc(host));
		if (dcHosts != null)
			dcHosts.remove(host);

		if (this.hostDownSwitchThreshold <= 0 && !switchedToBackupDc) {
			// if we lost as many nodes in the local dc as configured in the
			// threshold, switch to backup DC and never go back to local DC
			switchToBackup();

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
	
	private void switchToBackup(){
		switchedToBackupDc = true;
		switchedToBackupDcAt = new Date();
		logger.warn(
				"Lost {} nodes in data-center '{}'. Permanently switching to data-center '{}'",
				this.initHostDownSwitchThreshold, this.localDc,
				this.backupDc);
		
	}
}
