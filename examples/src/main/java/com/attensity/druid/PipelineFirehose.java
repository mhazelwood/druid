/**
 *
 */
package com.attensity.druid;

import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.json.JSONException;
import org.json.JSONObject;

import com.metamx.common.logger.Logger;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.providers.grizzly.GrizzlyAsyncHttpProvider;
import com.ning.http.client.websocket.WebSocket;
import com.ning.http.client.websocket.WebSocketCloseCodeReasonListener;
import com.ning.http.client.websocket.WebSocketListener;
import com.ning.http.client.websocket.WebSocketPongListener;
import com.ning.http.client.websocket.WebSocketTextListener;
import com.ning.http.client.websocket.WebSocketUpgradeHandler;

/**
 * @author mhazelwood
 *
 */
public class PipelineFirehose implements Firehose, WebSocketListener,
    WebSocketTextListener, WebSocketPongListener,
    WebSocketCloseCodeReasonListener {
  private static final Logger log = new Logger(PipelineFirehose.class);
  private final int QUEUE_SIZE = 2000;
  private final int CONNECTION_TIMEOUT = 60 * 1000;
  private final int IDLE_CONNECTION_TIMEOUT = 24 * 60 * 60 * 1000; // reconnect
                                    // if no
                                    // messages
                                    // in 24
                                    // hours
  private final int MAX_CONNECTION_RETRIES = 10 * 60 / 5; // retry failed
                              // connections for
                              // 10 minutes
                              // (attempting every
                              // 5 seconds)

  /**
   * max events to receive, -1 is infinite, 0 means nothing is delivered; use
   * this to prevent infinite space consumption or to prevent Attensity
   * Pipeline from being overloaded at an inconvenient time or to see what
   * happens when Pipeline stops delivering values, or to have hasMore()
   * return false.
   */
  private final int maxEventCount;

  /**
   * maximum number of minutes to fetch Pipeline articles. Use this to limit
   * run time. If zero or less, no time limit for run.
   */
  private final int maxRunMinutes;

  /**
   * URL to connect to for the web socket feed
   */
  private final String webSocketUrl;

  private AsyncHttpClientConfig websocketClientConfig;
  private AsyncHttpClient websocketClient;
  private WebSocket websocketConnection;
  private AsyncHttpClientConfig.Builder websocketConfigBuilder;
  private ArrayBlockingQueue<String> queue;
  private final LinkedList<String> dimensions;
  private long startMsec;
  private Thread onMessageThread;
  private Thread maxMinTimer;
  private Thread connectionTester;
  private String incompleteMessage = null;
  protected int connectionMessageCount = 0;
  private volatile long lastWebSocketInteraction;
  protected volatile long lastMessageRecieved;
  private volatile boolean closed;
  private long rowCount = 0;
    private final Map<String, Object> theMap = new HashMap<String, Object>(2);
  private final Runnable doNothingRunnable = new Runnable() {
    public void run() {
    }
  };

  public PipelineFirehose(int maxEventCount, int maxRunMinutes,
      String webSocketUrl) {
    this.maxEventCount = maxEventCount;
    this.maxRunMinutes = maxRunMinutes;
    this.webSocketUrl = webSocketUrl;
    this.dimensions = new LinkedList<String>();
  }

  public PipelineFirehose connect() throws IOException {

    /**
     * This queue is used to move Pipeline articles from the websocket feed
     * to the druid ingest thread.
     */
    queue = new ArrayBlockingQueue<String>(QUEUE_SIZE);
    startMsec = System.currentTimeMillis();


    dimensions.add("htags");
    dimensions.add("lang");
    dimensions.add("utc_offset");

    //
    // set up Pipeline
    //

    websocketConfigBuilder = new AsyncHttpClientConfig.Builder()
        .setConnectionTimeoutInMs(CONNECTION_TIMEOUT)
        .setIdleConnectionTimeoutInMs(IDLE_CONNECTION_TIMEOUT)
        .setSSLContext(createTrustAnySSLContext());

    openConnection();

    closed = false;
    if (maxRunMinutes > 0) {
      maxMinTimer = new Thread("PipelineInputHandler Timer") {
        @Override
        public void run() {
          try {
            Thread.sleep(1000L * 60L * maxRunMinutes);
            log.info("Max run time reached: " + maxRunMinutes
                + " minutes.  Stopping input.");
            closed = true;
          } catch (InterruptedException ignore) {
          }
        }
      };
      maxMinTimer.start();
    } else {
      maxMinTimer = null;
    }
    lastWebSocketInteraction = System.currentTimeMillis();
    lastMessageRecieved = System.currentTimeMillis();
    // create a thread to periodically test the connection to the socket
    // server
    connectionTester = new Thread("PiplineInputHandler ConnectionTester") {
      @Override
      public void run() {
        while (!closed) {
          try {
            // sleep for 15 seconds
            Thread.sleep(1000 * 15);
            if (closed) {
              break;
            }
            long secsSinceLastMessage = (System.currentTimeMillis() - lastWebSocketInteraction) / 1000;

            if (secsSinceLastMessage > (15 * 60)) {
              // no activity for over 15 minutes. force a
              // reconnect
              log.warn("No messages from socket server in last "
                  + secsSinceLastMessage
                  + " seconds despite trying to ping server.  Reconnecting...");
              closeAndReconnect();
            } else if (secsSinceLastMessage > 30) {
              // no activity for over 30 seconds. send a ping to
              // test the connection
              try {
                sendPing();
              } catch (Exception e) {
                // cannot send ping message. force a reconnect.
                log.warn("Unable to ping server.  Will reconnect.  Error: "
                    + e);
                closeAndReconnect();
              }
            }
          } catch (InterruptedException ignore) {
          }
        }
      }
    };
    connectionTester.start();

    return this;
  }

  // Performs only the initial open connection.
  protected void openConnection() throws IOException {
    websocketClientConfig = websocketConfigBuilder.build();
    websocketClient = new AsyncHttpClient(new GrizzlyAsyncHttpProvider(
        websocketClientConfig));

    WebSocketUpgradeHandler.Builder websocketBuilder = new WebSocketUpgradeHandler.Builder()
        .addWebSocketListener(this)
    // .setMaxTextSize( maxWebSocketTextMessageSize ) -- it seems to ignore
    // this setting
    ;
    int numRetries = 0;
    while (true) {
      try {
        websocketConnection = websocketClient.prepareGet(webSocketUrl)
            .execute(websocketBuilder.build()).get();
        break;
      } catch (Exception e) {
        if (++numRetries < MAX_CONNECTION_RETRIES) {
          log.warn("Error connecting to Pipeline.  Will try again in 5 seconds.  Error message="
              + e.getMessage());
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ignore) {
          }
        } else {
          // reached maximum retries
          throw new IOException(e);
        }
      }
    }
  }

  protected void closeConnection() {
    try {
      if (websocketConnection != null && websocketConnection.isOpen()) {
        websocketConnection.close();
      }
      if (websocketClient != null && !websocketClient.isClosed()) {
        websocketClient.close();
      }
      if (websocketClientConfig != null
          && !websocketClientConfig.isClosed()) {
        websocketClientConfig.executorService().shutdownNow();
        websocketClientConfig.reaper().shutdown();
      }
      try {
        onMessageThread.interrupt();
      } catch (Exception ignore) {
      }
    } catch (Exception e) {
      log.warn("Error closing websocket connection");
    }
  }

  protected void closeAndReconnect() {
    WebSocket connectionToClose = websocketConnection;
    // null out the instance variable so that we don't get any closing
    // messages
    websocketConnection = null;
    try {
      if (connectionToClose != null && connectionToClose.isOpen()) {
        connectionToClose.close();
      }
    } catch (Exception e) {
      log.debug("Error closing connection", e);
    }
    try {
      // Interrupt any messages that are blocked on adding to the queue.
      // This is needed because the connection doesn't actually close
      // until messages currently being processed are finished.
      onMessageThread.interrupt();
    } catch (Exception ignore) {
    }
    try {
      // Sleep long enough for the connection to close
      Thread.sleep(500);
    } catch (InterruptedException ignore) {
    }
    // websocketConnection is null, so reconnect will create a new
    // connection
    reconnect();
  }

  private synchronized void reconnect() {
    incompleteMessage = null;
    connectionMessageCount = 0;
    long twoMinutes = 1000 * 60 * 2;// ten minutes in milliseconds
    long lastMessage = 0;
    long firstMessage = 0;
    while (!closed
        && (websocketConnection == null || !websocketConnection
            .isOpen())) {
      if (isLongTimeSinceLastMessage()) {
        String message = "No new messages in the last hour, you may want to validate that your topic is working, and that you have no network connectivity problems";
        lastMessageRecieved = System.currentTimeMillis(); // reset this
                                  // so that
                                  // we won't
                                  // get this
                                  // message
                                  // for
                                  // another
                                  // hour
        log.warn(message);
      }
      long now = System.currentTimeMillis();
      boolean printMessages = now - lastMessage > twoMinutes;

      // reset lastMessageReceived to current time so that the
      // connectionTester thread is happy
      lastWebSocketInteraction = now;
      try {
        URI uri = new URI(webSocketUrl);
        if (printMessages) {
          log.info("Will attempt to reconnect to Pipeline server in 5 seconds...");
        }
        Thread.sleep(5000);
        if (!closed
            && (websocketConnection == null || !websocketConnection
                .isOpen())) {
          if (printMessages) {
            log.info("Attempting to reconnect to Pipeline using URI: "
                + uri.toString());
          }
          // Create a lister wrapper that will only pass along
          // callbacks if the
          // websocketConnection hasn't changed. This is needed to
          // prevent duplicates
          // coming from a previous connection that should have been
          // closed.
          WebSocketUpgradeHandler.Builder websocketBuilder = new WebSocketUpgradeHandler.Builder()
              .addWebSocketListener(this)
          // .setMaxTextSize( maxWebSocketTextMessageSize ) -- it
          // seems to ignore this setting
          ;
          websocketConnection = websocketClient
              .prepareGet(webSocketUrl)
              .execute(websocketBuilder.build()).get();
          // if we got this far, then it did reconnect
          // always print this message since we will be breaking out
          // of the loop and this won't get repeated
          log.info("Connection to Pipeline server re-established successfully.");
          break;
        }
      } catch (Exception e) {
        if (lastMessage == 0) {
          // only log the full stack trace the first time so that we
          // don't fill the logs.
          log.warn("Unable to connect to Pipeline", e);
          lastMessage = now;
          firstMessage = now;
        } else {
          // if the network is down or something, the log will get
          // full of this message
          // only print it out if has been 2 minutes since the last
          // time we printed it out
          if (printMessages) {
            long totalAttemptTime = (now - firstMessage)
                / (60 * 1000);
            log.warn("Unable to connect to Pipeline for the last "
                + totalAttemptTime + " minutes.");
            lastMessage = now;
          }
        }
        // loop and try again
      }
    }
  }

  // Start of Druid methods \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
  /*
   * (non-Javadoc)
   *
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    log.info(new Throwable("CLOSE pipeline"),"CLOSE pipeline");
    closed = true;
    if (queue != null) {
      queue.clear();
    }
    try {
      maxMinTimer.interrupt();
    } catch (Throwable ignore) {
    }
    try {
      connectionTester.interrupt();
    } catch (Throwable ignore) {
    }
    closeConnection();
  }

  /*
   * (non-Javadoc)
   *
   * @see io.druid.data.input.Firehose#hasMore()
   */
  @Override
  public boolean hasMore() {
    if (maxCountReached() || maxTimeReached()) {
      log.info("No more articles.  maxCountReached="+maxCountReached()+", maxTimeReached="+maxTimeReached());
      return false;
    } else {
      return true;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see io.druid.data.input.Firehose#nextRow()
   */
  @Override
  public InputRow nextRow() {

    // Interrupted to stop?
    if (Thread.currentThread().isInterrupted()) {
      throw new RuntimeException("Interrupted, time to stop");
    }

    // all done?
    if (maxCountReached() || maxTimeReached()) {
      // allow this event through, and the next hasMore() call will be
      // false
    }
    if (++rowCount % 1000 == 0) {
      log.info("nextRow() has returned %,d InputRows", rowCount);
    }

    String jsonText;
    try {
      jsonText = queue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException("InterruptedException", e);
    }

    long publishedAt;

    try {
      // parse the json text
      JSONObject json = new JSONObject( jsonText );

      // put fields into the map
      theMap.put("lang", json.getString("lang"));
      theMap.put("title", json.getString("title"));
      theMap.put("body", json.optString("body"));
      theMap.put("uri", json.getString("uri"));
      theMap.put("published_at", json.getString("published_at"));
      theMap.put("content_type", json.getString("content_type"));
      theMap.put("content_subtype", json.getString("content_subtype"));

      JSONObject metrics = json.optJSONObject("metrics");
      if(metrics==null) {
        theMap.put("followers", 0);
        theMap.put("following", 0);
        theMap.put("klout", 0.0f);
        theMap.put("status_updates", 0);
      } else {
        theMap.put("followers", metrics.optInt("followers", 0));
        theMap.put("following", metrics.optInt("following", 0));
        theMap.put("klout", (float)metrics.optDouble("klout", 0.0f));
        theMap.put("status_updates", metrics.optInt("status_updates", 0));
      }

      publishedAt = parseDate(json.getString("published_at")).getTime();
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }

    return new MapBasedInputRow(publishedAt, dimensions, theMap);
  }

  /*
   * (non-Javadoc)
   *
   * @see io.druid.data.input.Firehose#commit()
   */
  @Override
  public Runnable commit() {
    // ephemera in, ephemera out.
    return doNothingRunnable; // reuse the same object each time
  }

  // End of Duid methods /////////////////////////////////////

  // WebSocket call back methods \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  @Override
  public void onPong(byte[] data) {
    // this method is called by the socket server in response to the ping
    // message sent from the sendPing() method in this class.

    // update lastMessageReceived variable since we are still communicating
    // with the server
    lastWebSocketInteraction = System.currentTimeMillis();
  }

  @Override
  public void onFragment(String fragment, boolean last) {
    if (log.isDebugEnabled()) {
      log.debug("Fragment - "
          + (last ? "last" : "----")
          + " - "
          + (fragment.length() > 30 ? fragment.substring(0, 30)
              : fragment));
    }
    if (incompleteMessage == null) {
      incompleteMessage = fragment;
    } else {
      incompleteMessage += fragment;
    }
    if (last) {
      String message = incompleteMessage;
      incompleteMessage = null;
      onMessage(message);
    }
  }

  @Override
  public void onMessage(String text) {
    lastWebSocketInteraction = System.currentTimeMillis();
    lastMessageRecieved = lastWebSocketInteraction;
    incompleteMessage = null;
    onMessageThread = Thread.currentThread();
    connectionMessageCount++;

    if (log.isDebugEnabled()) {
      log.debug("Message - "
          + (text.length() > 41 ? text.substring(0, 41) : text));
      if (connectionMessageCount % 100 == 0) {
        log.debug("Current connection message count: "
            + connectionMessageCount);
      }
      log.debug("Size=" + text.length() + ", text=" + text);
    }

    if (maxCountReached()) {
      return;
    }
    addArticleToQueue(text);
  }

  private synchronized void addArticleToQueue(String text) {
    // Add to the queue. If it is full, check periodically to see
    // if it has been stopped.
    while (!closed && !maxTimeReached()) {
      try {
        boolean successful = queue.offer(text, 500,
            TimeUnit.MILLISECONDS);
        if (successful) {
          break;
        }
      } catch (InterruptedException ignore) {
        return;
      }
    }
  }

  @Override
  public void onClose(WebSocket w) {
  }

  @Override
  public void onClose(WebSocket websocket, int closeCode, String message) {
    log.debug("Connection Message Count: " + connectionMessageCount);
    lastWebSocketInteraction = System.currentTimeMillis();
    incompleteMessage = null;

    String codeMessage = "Code " + closeCode
        + (message != null ? ": " + message : "");
    if (closed) {
      // connection closed by a call to the closed() method in this class
      log.info("Connection closed. " + codeMessage);
    } else if (websocketConnection != null) {
      connectionMessageCount = 0;

      // connection closed by the server
      log.warn("Connection closed by server. " + codeMessage);
      closeAndReconnect();
    }
  }

  @Override
  public void onError(Throwable t) {
    // don't log these as errors because they are generated when
    // the connection is closed or when the socket server switches from
    // historic to real-time data.
    log.debug("WebSocket Error", t);
    lastWebSocketInteraction = System.currentTimeMillis();
    incompleteMessage = null;
  }

  @Override
  public void onOpen(WebSocket w) {
    lastWebSocketInteraction = System.currentTimeMillis();
    log.debug("Connection opened");
    incompleteMessage = null;
  }

  // end of WebSocket call back methods ///////////////////////////////////

  private boolean maxTimeReached() {
    if (maxRunMinutes <= 0) {
      return false;
    } else {
      if( (System.currentTimeMillis() - startMsec) / 60000L >= maxRunMinutes ) {
        log.info("Max time reached.  currentTimeMillis="+System.currentTimeMillis()+", startMsec="+startMsec+", maxRunMinutes="+maxRunMinutes);
        return true;
      } else {
        return false;
      }
    }
  }

  private boolean maxCountReached() {
    if (maxEventCount >= 0 && rowCount >= maxEventCount) {
      log.info("Max count reached.  rowCount="+rowCount+", maxEventCount="+maxEventCount);
      return true;
    } else {
      return false;
    }
  }

  private boolean isLongTimeSinceLastMessage() {
    return System.currentTimeMillis() - lastMessageRecieved > 1000 * 60 * 60; // one
                                          // hour
                                          // in
                                          // milliseconds
  }

  private void sendPing() throws IOException {
    if (websocketConnection != null) {
      // Send a PING to the socket server. It should respond with a pong
      // to the onControl method in this class.
      websocketConnection.sendPing("ping".getBytes());
    }
  }

  private SSLContext createTrustAnySSLContext() throws IOException {
    try {
      // Create a trust manager that does not validate certificate chains
      TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        public void checkClientTrusted(
            java.security.cert.X509Certificate[] certs,
            String authType) {
        }

        public void checkServerTrusted(
            java.security.cert.X509Certificate[] certs,
            String authType) {
        }
      } };

      // create a fast SecureRandom
      long seed = new Date().getTime();
      SecureRandom secureRandom = SecureRandom.getInstance(
          "SHA1PRNG", "SUN"); //$NON-NLS-1$ //$NON-NLS-2$
      secureRandom.setSeed(seed);

      // create the socket factory
      SSLContext sc = SSLContext.getInstance("SSL"); //$NON-NLS-1$
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      return sc;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }



    private Date parseDate( String value ) {
        // 2012-02-08T17:15:27Z
        String[] dateFormats = {"yyyy-MM-dd'T'HH:mm:ss'Z'","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"};
        for(String format : dateFormats) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat( format );
                formatter.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
                formatter.setLenient( true );
                return formatter.parse( value );
            } catch (Exception ignore) {}
        }
        log.warn( "Unable to parse date: "+value );
        return new Date();
    }

}
