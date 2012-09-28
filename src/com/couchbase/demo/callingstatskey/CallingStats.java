
package com.couchbase.demo.callingstatskey;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.vbucket.VBucketNodeLocator;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Get some stats from Couchbase Server for a key.
 * 
 * @author ingenthr
 */
public class CallingStats {

  /**
   * A simple test file to demonstrate getting stats for a given key from
   * Couchbase Server.
   * 
   * @param args the command line arguments
   * @throws URISyntaxException 
   * @throws ExecutionException
   * @throws InterruptedException 
   * @throws IOException  
   */
  public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ExecutionException {
    List<URI> baselist = new ArrayList<URI>();
    
    URI firstHost = new URI("http://192.168.1.200:8091/pools");
    
    baselist.add(firstHost);
    
    CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
    cfb.setOpTimeout(10000);
    
    CouchbaseClient cbclient;
    cbclient = new CouchbaseClient(cfb.buildCouchbaseConnection(baselist, "PERSISTENT", ""));
    
    // Now we'll delete it if it's in there, then set it.
    cbclient.delete("foo").get();
    
    printKeyStats(cbclient, "foo");
    
    assert cbclient.set("foo", 0, "bar").get() : "could not set foo";
    printKeyStats(cbclient, "foo");
    
    // after a bit of time, we should see the key's not dirty, etc.
    Thread.sleep(200);
    printKeyStats(cbclient, "foo");

    
    
    cbclient.shutdown(10, TimeUnit.SECONDS);
    
  }

  private static void printKeyStats(CouchbaseClient client, String key) {
    // the key stats require the vbucket on the end of the argument, so look up
    // the vbucket index    
    int vbucketIndex = getVbucketIndex(client, key);
    System.err.println("key " + key + " " + vbucketIndex);
    Map<SocketAddress, Map<String, String>> stats = client.getStats("key " + key + " " + vbucketIndex);
    
    // loop, print, loop, print.
    // seems excessive, but stats is a broadcast operation and only one server
    // will be primary for the key
    for (Map.Entry<SocketAddress, Map<String,String>> entry: stats.entrySet()){
      System.out.println("For host " + entry.getKey());
      for (Map.Entry<String, String> thestats : entry.getValue().entrySet()) {
        StringBuilder sb = new StringBuilder("");
        sb.append("Stat: ").append(thestats.getKey()).append(" Value: ").append(thestats.getValue());
        System.out.println(sb.toString());
      } 
    }
  }

  private static int getVbucketIndex(CouchbaseClient cbclient, String key) {
    // this is really intended internal, so pardon the casting
    VBucketNodeLocator nodeLocator = (VBucketNodeLocator)cbclient.getNodeLocator();
    int vBucketIndex = nodeLocator.getVBucketIndex(key);
    return vBucketIndex;
  }
}
