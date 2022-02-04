package de.tuda.dmdb.net;

import de.tuda.dmdb.storage.AbstractRecord;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of a Server component based on TCP. This class is used in the receive operator to
 * listen for and handle incoming records. The TCPServer dispatches new client connections to pool
 * of Handler-Threads (implemented using Java Executor Service)
 *
 * @author melhindi
 */
public class TCPServer extends Thread {

  private final ReceiveHandler handler; // Handler used for incoming client connections
  private final ServerSocket
      serverSocket; // Server socket to listen for incoming client connections
  private final ExecutorService pool; // Thread pool to handle client connections concurrently
  private final Queue<AbstractRecord>
      localCache; // Reference to the local cache which will be used to the handler threads
  private final AtomicInteger
      finishedPeers; // Reference to the counter for the registration of peers that finished sending
  // data
  private boolean
      running; // Flag to indicate when to stop the server (stop listening to incoming connections)

  /**
   * Constructor of TCPServer
   *
   * @param port - The port on which the server is supposed to listen for incoming connections
   * @param localCache - Reference to the local cache in which in coming Records will be stored
   *     (needs to be thread safe)
   * @param finishedPeers - Reference to the counter for the registration of peers that finished
   *     sending data
   * @throws IOException - Thrown when the ServerSocket could not be opened
   */
  public TCPServer(int port, Queue<AbstractRecord> localCache, AtomicInteger finishedPeers)
      throws IOException {
    handler = new ReceiveHandler(localCache, finishedPeers);
    serverSocket = new ServerSocket(port);
    pool = Executors.newCachedThreadPool();
    this.localCache = localCache;
    this.finishedPeers = finishedPeers;
    this.running = true;
  }

  @Override
  public void run() {
    try {
      // listen to incoming connections until stopServer is called (server socket is closed)
      while (running) {
        // listen on server socket
        System.out.println("TCPServer on " + serverSocket.getLocalSocketAddress() + " started!");
        Socket socket = serverSocket.accept();
        // dispatch incoming connection to thread pool
        handler.handle(socket, pool);
      }
    } catch (SocketException e) {
      // When stopServer() is called, a SocketException is thrown
    } catch (IOException e) {
      System.err.println("TCPServer - run(): Got the following exception:");
      e.printStackTrace();
    }
  }

  /** Stop the server (listening to incoming connections) and close the listener socket. */
  public void stopServer() {
    try {
      this.running = false;
      System.out.println(
          "TCPServer on " + serverSocket.getLocalSocketAddress() + " will be closed!");
      serverSocket.close();
    } catch (IOException e) {
      System.err.println("TCPServer - stopServer(): Got the following exception:");
      e.printStackTrace();
    }
    // stop active handler threads
    pool.shutdown();
  }

  public int getActiveConnectionsCount() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  /**
   * Get a reference to the Server's finishedPeers count
   *
   * @return the finishedPeers
   */
  public AtomicInteger getFinishedPeers() {
    return finishedPeers;
  }

  /**
   * Get a reference to the Server's localCache
   *
   * @return the localCache
   */
  public Queue<AbstractRecord> getLocalCache() {
    return localCache;
  }
}
