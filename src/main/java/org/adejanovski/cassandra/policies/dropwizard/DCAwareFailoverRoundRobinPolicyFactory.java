package org.adejanovski.cassandra.policies.dropwizard;

import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy;
import org.stuartgunter.dropwizard.cassandra.loadbalancing.*;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * A factory for configuring and building {@link com.datastax.driver.core.policies.DCAwareRoundRobinPolicy} instances.
 * <p/>
 * <b>Configuration Parameters:</b>
 * <table>
 *     <tr>
 *         <td>Name</td>
 *         <td>Default</td>
 *         <td>Description</td>
 *     </tr>
 *     <tr>
 *         <td>localDC</td>
 *         <td>No default.</td>
 *         <td>The name of the local datacenter (as known by Cassandra).</td>
 *     </tr>
 *     <tr>
 *         <td>backupDC</td>
 *         <td>No default.</td>
 *         <td>The name of the backup datacenter (as known by Cassandra).</td>
 *     </tr>
 * </table>
 */
@JsonTypeName("dcAwareFailoverRoundRobin")
public class DCAwareFailoverRoundRobinPolicyFactory implements LoadBalancingPolicyFactory {
	
	private String localDC;
	private String backupDC;
    private Integer hostDownSwitchThreshold;
	
    
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
    public Integer getHostDownSwitchThreshold() {
        return hostDownSwitchThreshold;
    }

    @JsonProperty
    public void setHostDownSwitchThreshold(Integer hostDownSwitchThreshold) {
        this.hostDownSwitchThreshold = hostDownSwitchThreshold;
    }
    
	public LoadBalancingPolicy build() {
		return new DCAwareFailoverRoundRobinPolicy(localDC, backupDC, hostDownSwitchThreshold);
	}

}
