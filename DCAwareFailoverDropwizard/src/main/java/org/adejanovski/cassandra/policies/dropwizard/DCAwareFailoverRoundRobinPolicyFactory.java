package org.adejanovski.cassandra.policies.dropwizard;

import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy;
import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy.InvalidConsistencyLevelException;
import org.stuartgunter.dropwizard.cassandra.loadbalancing.*;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * A factory for configuring and building
 * {@link com.datastax.driver.core.policies.DCAwareRoundRobinPolicy} instances.
 * <b>Configuration Parameters:</b>
 * <table>
 * <tr>
 * <td>Name</td>
 * <td>Default</td>
 * <td>Description</td>
 * </tr>
 * <tr>
 * <td>localDC</td>
 * <td>No default.</td>
 * <td>The name of the local datacenter (as known by Cassandra).</td>
 * </tr>
 * <tr>
 * <td>backupDC</td>
 * <td>No default.</td>
 * <td>The name of the backup datacenter (as known by Cassandra).</td>
 * </tr>
 * <tr>
 * <td>minimumAchievableConsistencyLevel</td>
 * <td>ConsistencyLevel.LOCAL_QUORUM</td>
 * <td>The minimum required consistency level that must be achievable. Below it, the switch is triggered.</td>
 * </tr>
 * <tr>
 * <td>switchBackDelayFactor</td>
 * <td>0</td>
 * <td>The connection can switch back if uptime &gt;= downtime*switchBackDelayFactor (gives time for hinted handoff to complete)</td>
 * </tr>
 * <tr>
 * <td>noSwitchBackDowntimeDelay</td>
 * <td>0</td>
 * <td>Switch back cannot happen if downtime &gt; noSwitchBackDowntimeDelay (in seconds)</td>
 * </tr>
 * </table>
 */
@JsonTypeName("dcAwareFailoverRoundRobin")
public class DCAwareFailoverRoundRobinPolicyFactory implements
		LoadBalancingPolicyFactory {

	private String localDC;
	private String backupDC;
	private ConsistencyLevel minimumAchievableConsistencyLevel;
	private Float switchBackDelayFactor;
	private Integer noSwitchBackDowntimeDelay;

	@JsonProperty
	public String getLocalDC() {
		return localDC;
	}

	@JsonProperty
	public void setLocalDC(String localDC) {
		this.localDC = localDC;
	}

	@JsonProperty
	public String getBackupDC() {
		return backupDC;
	}

	@JsonProperty
	public void setBackupDC(String backupDC) {
		this.backupDC = backupDC;
	}

	@JsonProperty
	public ConsistencyLevel getMinimumAchievableConsistencyLevel() {
		return minimumAchievableConsistencyLevel;
	}

	@JsonProperty
	public void setMinimumAchievableConsistencyLevel(ConsistencyLevel minimumAchievableConsistencyLevel) {
		this.minimumAchievableConsistencyLevel = minimumAchievableConsistencyLevel;
	}
	
	@JsonProperty
	public void setSwitchBackDelayFactor(Float switchBackDelayFactor) {
		this.switchBackDelayFactor = switchBackDelayFactor;
	}

	@JsonProperty
	public void setNoSwitchBackDowntimeDelay(Integer noSwitchBackDowntimeDelay) {
		this.noSwitchBackDowntimeDelay = noSwitchBackDowntimeDelay;
	}

	public LoadBalancingPolicy build() {		
		try {
			return new DCAwareFailoverRoundRobinPolicy(localDC, backupDC,
						minimumAchievableConsistencyLevel, switchBackDelayFactor, noSwitchBackDowntimeDelay);
		} catch (InvalidConsistencyLevelException e) {		
			e.printStackTrace();
			return null;
		}
		 
	}

}
