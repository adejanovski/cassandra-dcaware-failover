# DC Aware Failover Round Robin Policy for Datastax Java Driver

Properly handling failover in multi DC Cassandra clusters with LOCAL_* CL usually require writing specific code to properly process failover scenarios.

The Cassandra setups that fail delivering automatic DC failure handling are those with 2 DCs or those with 1+ real time DCs and 1+ analytical/search DC.

Having a 2 DC cluster and using CL.QUORUM will lead to systematic failure when a DC goes down. 
Using CL.ONE will lead to inconsistencies if a DC goes down and comes back up after a while.
Using CL.LOCAL_QUORUM with DCAwareRoundRobinPolicy will segregate the traffic to a single DC, but the load balancing policy will prevent switching to the other DC in case of failure (in its basic configuration).
This is a conservative behavior, because if done differently, when the primary DC comes back up it will probably not be up to date, and it could even be out of the hinted handoff window (which will require a full repair).
In case you have an analytical or a search DC, you must segregate traffic in order to guarantee low latencies, which forces the use of CL.LOCAL_* consistencies. The behavior in case of failure is the same as the one above.

Handling those failure scenarios has to be implemented in client applications, by doing the following :
* detect how many nodes are down in the local DC
* if too many nodes are down to allow queries to succeed (depends on the replication factor), the app has to switch to a backup dc (either having two connections and switching between them, either recreating a new connection with a different LB policy conf).
* Switching automatically back at any time to the local DC must be prevented because repair might be necessary in order to regain consistency
 
 
The DCAwareFailoverRoundRobinPolicy is based on the DCAwareRoundRobinPolicy and handles the above one way switch without having to code it. It takes the following parameters : 
* localDc : the primary DC of the app
* backupDc : the DC to switch to in case the primary DC loses a defined number of nodes
* hostDownSwitchThreshold : the number of lost nodes that will trigger the switch
* switchBackDelayFactor : The connection can switch back if uptime >= downtime*switchBackDelayFactor (gives time for hinted handoff to complete)
* noSwitchBackDowntimeDelay : Switch back cannot happen if downtime > noSwitchBackDowntimeDelay (in seconds) - usually the hinted handoff window (default in Cassandra is 3h)
  
This policy does a one way switch to the backup DC to prevent inconsistencies, and the connection must be recreated in order to switch back to the local DC (which should require either rebooting the app, or recreating the Cluster and Session objects), unless auto back switch conditions are fulfilled : 
* Local DC has regained enough nodes
* uptime is superior to downtime*switchBackDelayFactor
* downtime has been lower than noSwitchBackDowntimeDelay


# Usage

Include this project in your classpath and use it as any other load balancing policy when creating the cluster object:

```java
Cluster cluster = Cluster.builder()
			         .addContactPoints("127.0.0.1","127.0.0.2","127.0.0.3","127.0.0.4","127.0.0.5")
			         .withLoadBalancingPolicy(new org.adejanovski.cassandra.policies.DCAwareRoundRobinPolicy("primaryDc","backupDc",2))			        
			         .build();
```			         

The above will segregate coordination traffic to DC 'primaryDc' and if at least 2 nodes are lost in this DC, switch to 'backupDc' and never go back.

To authorize auto back switch you can add the switchBackDelayFactor and noSwitchBackDowntimeDelay : 

```java
Cluster cluster = Cluster.builder()
			         .addContactPoints("127.0.0.1","127.0.0.2","127.0.0.3","127.0.0.4","127.0.0.5")
			         .withLoadBalancingPolicy(new org.adejanovski.cassandra.policies.DCAwareRoundRobinPolicy("primaryDc","backupDc", 2, 1.5, 10800))			        
			         .build();
``` 

The above will allow automatically switching back if conditions are fulfilled : 
* downtime didn't last more than 3 hours (10800 seconds)
* uptime since local DC recovery has lasted more than downtime*1.5 (if downtime was 30 mn, back switch will be prevented for 45 mn after local DC recovery) 

# Usage in Dropwizard

The DCAwareFailoverRoundRobinPolicy is "dropwizard-ready".
 
Use it by declaring it in the yml config file, and it can be combined with any appropriate parent policy:

```xml
loadBalancingPolicy:
    type: tokenAware
    shuffleReplicas: true
    subPolicy:
      type: dcAwareFailoverRoundRobin
      localDC: primaryDc
      backupDC: backupDc
      hostDownSwitchThreshold: 2
      switchBackDelayFactor: 1.5
      noSwitchBackDowntimeDelay: 10800 
```

### Build status

[![Build Status](https://travis-ci.org/adejanovski/cassandra-dcaware-failover.svg)](https://travis-ci.org/adejanovski/cassandra-dcaware-failover)

			         
			         
