package org.adejanovski.cassandra.policies;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy;
import org.adejanovski.cassandra.policies.DCAwareFailoverRoundRobinPolicy.InvalidConsistencyLevelException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;

public class DCAwareFailoverRoundRobinPolicyTest {
	DCAwareFailoverRoundRobinPolicy policy;
	Collection<Host> hosts;
	Cluster cluster;

	@BeforeMethod
	public void setUp() throws UnknownHostException, InvalidConsistencyLevelException {
		hosts = new ArrayList<Host>();
		policy = new DCAwareFailoverRoundRobinPolicy("dc1", "dc2", ConsistencyLevel.LOCAL_QUORUM);
		for(int i=0;i<6;i++){
			Host host = Mockito.mock(Host.class);
			InetAddress address = InetAddress.getByName("127.0.0." + i);
			Mockito.when(host.getAddress()).thenReturn(address);
			if(i<=2){
				Mockito.when(host.getDatacenter()).thenReturn("dc1");				
			}else{
				Mockito.when(host.getDatacenter()).thenReturn("dc2");
			}
			hosts.add(host);			
		}		
		
		cluster = Mockito.mock(Cluster.class);
		policy.init(cluster, hosts);
		
	}
		

	@Test
	public void testDistance() throws UnknownHostException{
		int i=0;
		for(Host host:hosts){			
			policy.onUp(host);
			if(i<=2){
				// dc1 is local
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}else{
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));
			}
			i++;			
		}
	}
	
	//@Test
	public void testLostOneNode() throws UnknownHostException{
		int i=0;		
		for(Host host:hosts){
			if(i==0){
				// we lost the first node
				// which doesn't trigger the switch
				policy.onDown(host);
			}
			
			if(i<=2){
				// dc1 is local
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}else{
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));
			}
			i++;			
		}
	}
	
	
	//@Test
	public void testLostTwoNodes() throws UnknownHostException{
		int i=0;		
		
		for(Host host:hosts){
			if(i<=1){
				// we lost the first node
				// which doesn't trigger the switch
				policy.onDown(host);
			}
			i++;
		}
			
		i=0;
		for(Host host:hosts){	
			if(i<=2){
				// dc1 is remote now (lost 2 nodes)
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));				
			}else{
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}
			i++;			
		}
	}
	
	//@Test
	public void testSwitchBackProtection() throws UnknownHostException{
		int i=0;		
		
		for(Host host:hosts){
			if(i<=1){
				// we lost the first node
				// which doesn't trigger the switch
				policy.onDown(host);
			}
			i++;
		}
				
		for(Host host:hosts){
			// first host is back up
			policy.onUp(host);
			break;
		}
		
			
		i=0;
		for(Host host:hosts){	
			if(i<=2){
				// dc1 is still remote now (lost 2 nodes, and got 1 back)
				assertTrue(policy.distance(host).equals(HostDistance.REMOTE) || policy.distance(host).equals(HostDistance.IGNORED));				
			}else{
				assertEquals(policy.distance(host),HostDistance.LOCAL);
			}
			i++;			
		}
	}
}
