package org.adejanovski.cassandra.policies.dropwizard;

import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy;
import org.stuartgunter.dropwizard.cassandra.loadbalancing.*;

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
	private Integer tokenReplicaLostSwitchThreshold;
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
	public Integer getTokenReplicaLostSwitchThreshold() {
		return tokenReplicaLostSwitchThreshold;
	}

	@JsonProperty
	public void setTokenReplicaLostSwitchThreshold(Integer tokenReplicaLostSwitchThreshold) {
		this.tokenReplicaLostSwitchThreshold = tokenReplicaLostSwitchThreshold;
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
		return new DCAwareFailoverRoundRobinPolicy(localDC, backupDC,
				tokenReplicaLostSwitchThreshold, switchBackDelayFactor, noSwitchBackDowntimeDelay);
	}

}
