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


    if (args.length == 0) {
      System.err.println("usage: java -jar CallingStats.jar ipaddr [ipaddr ...]");
      System.exit(1);
    }

    List<URI> baselist = new ArrayList<URI>();

    for (String argument : args) {
      String oneHost = "http://" + argument + ":8091/pools";
      try {
        URI node = new URI(oneHost);
        baselist.add(node);

      } catch (URISyntaxException ex) {
        System.err.println("Could not create a URI from: " + argument);
        System.exit(1);
      }
    }
    CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
    cfb.setOpTimeout(10000);

    CouchbaseClient cbclient;
    cbclient = new CouchbaseClient(cfb.buildCouchbaseConnection(baselist, "PERSISTENT", ""));

    // Now we'll delete it if it's in there, then set it.
    cbclient.delete("foo").get();  // we don't care if the delete fails

    System.out.println("**** Stats before set:");
    printKeyStats(cbclient, "foo");

    System.out.println("**** Stats after set:");
    if(!cbclient.set("foo", 0, "bar").get()) {
      System.err.println("Could not set foo.");
      System.exit(1);
    }
    printKeyStats(cbclient, "foo");

    // after a bit of time, we should see the key's not dirty, etc.
    Thread.sleep(10000);
    System.out.println("**** Stats after set and sleep:");
    printKeyStats(cbclient, "foo");



    cbclient.shutdown(10, TimeUnit.SECONDS);

  }

  private static void printKeyStats(CouchbaseClient client, String key) {
    VBucketNodeLocator nodeLocator = (VBucketNodeLocator) client.getNodeLocator();
    int vBucketIndex = nodeLocator.getVBucketIndex(key);
    // the key stats require the vbucket on the end of the argument, so look up
    // the vbucket index    
    int vbucketIndex = vBucketIndex;

    Map<SocketAddress, Map<String, String>> stats = client.getStats("key " + key + " " + vbucketIndex);

    // since the stats is called for all nodes, just reach into the one we really care about
    SocketAddress primaryNode = nodeLocator.getPrimary(key).getSocketAddress();
    Map<String, String> statsForKey = stats.get(primaryNode);

    for (Map.Entry<String, String> thestats : statsForKey.entrySet()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Stat: ").append(thestats.getKey())
              .append(" Value: ").append(thestats.getValue());
      System.out.println(sb.toString());
    }
  }
}
