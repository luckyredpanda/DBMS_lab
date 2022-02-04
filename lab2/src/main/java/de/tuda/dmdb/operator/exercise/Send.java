package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.net.TCPClient;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.SendBase;
import de.tuda.dmdb.storage.AbstractRecord;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of send operator
 *
 * @author melhindi
 */
public class Send extends SendBase {
  boolean f = true;
  int num = -1;
  List<AbstractRecord> local_e = new ArrayList<AbstractRecord>();

  /**
   * Constructor of Send
   *
   * @param child                Child operator used to process next calls, e.g., TableScan or Selection
   * @param nodeId               Own nodeId to identify which records to keep locally
   * @param nodeMap              Map containing connection information (as "IP:port" or "domain-name:port") to
   *                             establish connection to other peers
   * @param distributionFunction Function to determine where to send a record to
   */
  public Send(
          Operator child,
          int nodeId,
          Map<Integer, String> nodeMap,
          Function<AbstractRecord, List<Integer>> distributionFunction) {
    super(child, nodeId, nodeMap, distributionFunction);
  }

  @Override
  public void open() {
    // TODO: implement this method
    // init child
    child.open();
    String [] node;
    // create a client socket for all peer nodes using information in nodeMap
    for (Map.Entry<Integer, String> entry : nodeMap.entrySet()) {
      if (entry.getKey() != nodeId) {
        try {
          node = entry.getValue().split(":");
          String host_s = node[0];
          int port_s = Integer.parseInt(node[1]);
          TCPClient tc = new TCPClient(host_s, port_s, nodeId, entry.getKey());
          connectionMap.put(entry.getKey(), tc);
          //System.out.println("cm"+connectionMap);
        } catch (UnknownHostException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public AbstractRecord next() {
    // TODO: implement this method
    // retrieve next from child
    // invoke hash-function to re-partition record
    // keep in mind that distributionFunction can tell us to send to remote nodes
    // and ourself
    // first send to remote nodes and in the end return local element
    AbstractRecord r = child.next();

    while (r!=null) {
      List<Integer> l = distributionFunction.apply(r);
      for (int i = 0; i < l.size(); i++) {
        Integer l1 = l.get(i);
        for(int j = i;j < l.size(); j++){
          Integer l2 = l.get(j);
          if (connectionMap.containsKey(l2) && nodeId != l2) {
            TCPClient tc = connectionMap.get(l2);
            tc.sendRecord(r);
            local_e.add(r);
          }
        }
        if (nodeId == l1) {
          local_e.add(r);
          return r;
        }
      }
      r = child.next();
    }
    this.closeConnectionsToPeers();
    return null;
  }


  @Override
  public void close() {
    // TODO: implement this method
    this.closeConnectionsToPeers();
    child.close();
  }
}
