# DC Aware Failover Round Robin Policy for Datastax Java Driver

Properly handling failover in multi DC Cassandra clusters with LOCAL_* CL usually require writing specific code to properly process failover scenarios.

The Cassandra setups that fail delivering automatic DC failure handling are those with 2 DCs or those with 1+ real time DCs and 1+ analytical/search DC.

In the case with a 2 DCs cluster, using CL.QUORUM will lead to systematic failure when a DC goes down. Using CL.ONE will lead to inconsitencies if a DC goes down and comes back up after a while.
Using CL.LOCAL_QUORUM with DCAwareRoundRobinPolicy will segregate the traffic to a single DC, but the load balancing policy will prevent switching to the other DC in case of failure (in its basic configuration).
This is a conservative behavior, because if done differently, when the primary DC comes back up it will probably not be up to date, and it could even be out of the hinted handoff window (which will require a full repair).
In case you have an analytical or a search DC, you must segregate traffic in order to guarantee low latencies, which forces the use of CL.LOCAL_* consistencies. The behavior in case of failure is the same as the one above.

Handling those failure scenarios has to be implemented in client applications, by doing the following :
* detect how many nodes are down in the local DC
* if too many nodes are down to allow queries to succeed (depends on the replication factor), the app has to switch to a backup dc (either having two connections and switching between them, either recreating a new connection with a different LB policy conf).
* Switching automatically back to the local DC must be prevented because repair might be necessary in order to regain consistency
 
 
The DCAwareFailoverRoundRobinPolicy is based on the DCAwareRoundRobinPolicy, but takes the following parameters : 
* localDc : the primary DC of the app
* backupDc : the DC to switch to in case the primary DC loses a defined number of nodes
* hostDownSwitchThreshold : the number of lost nodes that will trigger the switch
  
This policy does a one way switch to the backup DC to prevent inconsistencies, and the connection must be recreated in order to switch back to the local DC (which should require either rebooting the app, or recreating the Cluster and Session objects).


# Usage

Include this project in your classpath and use it as any other load balancing policy when creating the cluster object:

```java
Cluster cluster2 = Cluster.builder()
			         .addContactPoints("127.0.0.1","127.0.0.2","127.0.0.3","127.0.0.4","127.0.0.5")
			         .withLoadBalancingPolicy(new org.adejanovski.cassandra.policies.DCAwareRoundRobinPolicy("primaryDc","backupDc",2))			        
			         .build();
```			         

The above will segregate coordination traffic to DC 'primaryDc' and if at least 2 nodes are lost in this DC, switch to 'backupDc' and never go back.


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
```

			         
			         