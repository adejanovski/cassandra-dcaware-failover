package org.adejanovski.cassandra.policies.dropwizard;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy;
import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy.Builder;
import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy.InvalidConsistencyLevelException;
import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy.KeyspaceTokenRange;
import org.adejanovski.cassandra.policies.FailoverSwitchCallback;

import systems.composable.dropwizard.cassandra.loadbalancing.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

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
 * <td>minimumRequiredConsistencyLevel</td>
 * <td>ConsistencyLevel.LOCAL_QUORUM</td>
 * <td>The minimum required consistency level that must be achievable. Below it,
 * the switch is triggered.</td>
 * </tr>
 * <tr>
 * <td>switchBackDelayFactor</td>
 * <td>0</td>
 * <td>The connection can switch back if uptime &gt;=
 * downtime*switchBackDelayFactor (gives time for hinted handoff to complete)</td>
 * </tr>
 * <tr>
 * <td>noSwitchBackDowntimeDelay</td>
 * <td>0</td>
 * <td>Switch back cannot happen if downtime &gt; noSwitchBackDowntimeDelay (in
 * seconds)</td>
 * </tr>
 * <tr>
 * <td>keyspace</td>
 * <td></td>
 * <td>The specific keyspace that is monitored to trigger switches</td>
 * </tr>

 * </table>
 */
@JsonTypeName("dcAwareFailoverRoundRobin")
public class DCAwareFailoverRoundRobinPolicyFactory implements
		LoadBalancingPolicyFactory {

	private static MetricRegistry metrics = new MetricRegistry();
	private UUID uuid = UUID.randomUUID();
	private String currentDC;
	private Map<KeyspaceTokenRange, ConsistencyLevel> maxAchievableConsistencyLevelPerKs;
	private String localDC;
	private String backupDC;
	private String minimumRequiredConsistencyLevel;
	private Float switchBackDelayFactor;
	private Integer noSwitchBackDowntimeDelay;
	private String keyspace;
	private boolean switchBackCanNeverHappen=false; 
	private Date switchedToBackupDcAt;
	private Date localDcCameBackUpAt;

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
	public String getMinimumRequiredConsistencyLevel() {
		return minimumRequiredConsistencyLevel;
	}

	@JsonProperty
	public void setMinimumRequiredConsistencyLevel(
			String minimumRequiredConsistencyLevel) {
		this.minimumRequiredConsistencyLevel = minimumRequiredConsistencyLevel;
	}

	@JsonProperty
	public void setSwitchBackDelayFactor(Float switchBackDelayFactor) {
		this.switchBackDelayFactor = switchBackDelayFactor;
	}

	@JsonProperty
	public void setNoSwitchBackDowntimeDelay(Integer noSwitchBackDowntimeDelay) {
		this.noSwitchBackDowntimeDelay = noSwitchBackDowntimeDelay;
	}

	@JsonProperty
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public static MetricRegistry metrics() {
		return metrics;
	}

	public LoadBalancingPolicy build() {
		try {

			SwitchCallback callback = new SwitchCallback();

			this.currentDC = this.localDC;

			Builder builder = DCAwareFailoverRoundRobinPolicy
					.builder()
					.withLocalDc(localDC)
					.withBackupDc(backupDC)
					.withMinimumRequiredConsistencyLevel(
							ConsistencyLevel
									.valueOf(minimumRequiredConsistencyLevel))
					.withSwitchBackDelayFactor(switchBackDelayFactor)
					.withNoSwitchBackDowntimeDelay(noSwitchBackDowntimeDelay)
					.withFailoverSwitchCallback(callback);

			if(this.keyspace != null && !this.keyspace.equals("")){
				builder.withMonitoredKeyspace(keyspace);
			}
			
			DCAwareFailoverRoundRobinPolicy policy = builder.build();

			initMetrics();

			return policy;
		} catch (InvalidConsistencyLevelException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}

	private void initMetrics() {
		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"current_dc"), new Gauge<String>() {
			@Override
			public String getValue() {
				return currentDC;
			}
		});

		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"local_dc"), new Gauge<String>() {
			@Override
			public String getValue() {
				return localDC;
			}
		});

		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"backup_dc"), new Gauge<String>() {
			@Override
			public String getValue() {
				return backupDC;
			}
		});

		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"minimum_consistency_level"), new Gauge<String>() {
			@Override
			public String getValue() {
				return minimumRequiredConsistencyLevel;
			}
		});

		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"switch_back_delay_factor"), new Gauge<Float>() {
			@Override
			public Float getValue() {
				return switchBackDelayFactor;
			}
		});

		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"no_switch_back_downtime_delay"), new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return noSwitchBackDowntimeDelay;
			}
		});

		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"max_achievable_cl_per_keyspace"),
				new Gauge<Map<KeyspaceTokenRange, ConsistencyLevel>>() {
					@Override
					public Map<KeyspaceTokenRange, ConsistencyLevel> getValue() {
						return maxAchievableConsistencyLevelPerKs;
					}
		});
		
		
		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"switch_back_can_never_happen"), new Gauge<Boolean>() {
			@Override
			public Boolean getValue() {
				return switchBackCanNeverHappen;
			}
		});

		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"switched_to_backup_DC_at"), new Gauge<Date>() {
			@Override
			public Date getValue() {
				return switchedToBackupDcAt;
			}
		});
		
		metrics.register(MetricRegistry.name(
				DCAwareFailoverRoundRobinPolicyFactory.class, uuid.toString(),
				"local_DC_came_back_up_at"), new Gauge<Date>() {
			@Override
			public Date getValue() {
				return localDcCameBackUpAt;
			}
		});

	}

	class SwitchCallback implements FailoverSwitchCallback {

		@Override
		public String getCallbackName() {
			return "Cassandra Failover Callback";
		}

		@Override
		public void switchDcCallback(
				String _currentDc,
				ConcurrentMap<KeyspaceTokenRange, ConsistencyLevel> _maxAchievableConsistencyPerKeyspace,
				ConcurrentMap<KeyspaceTokenRange, Set<String>> _lostTokenRanges,
				String _localDc, String _backupDc, Date _switchedToBackupDcAt) {
			currentDC = _currentDc;
			maxAchievableConsistencyLevelPerKs = _maxAchievableConsistencyPerKeyspace;
			switchedToBackupDcAt = _switchedToBackupDcAt;

		}

	}

}
