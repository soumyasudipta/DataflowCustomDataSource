/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package source;

import org.apache.beam.sdk.io.UnboundedSource;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;



@WebSocket
public class Socket extends UnboundedSource.UnboundedReader<String> {

  private static final Logger LOG = LoggerFactory.getLogger(Socket.class);
  private Source source;
  private String current;
  private Instant currentTimestamp;
  private Queue<String> queue;
  private String url;

// Uncomment if you want to use Subscribe Message  
//  private static final String COINBASE_SUBSCRIBE_MESSAGE =
//      "{\r\n" + 
//      "    \"type\": \"subscribe\",\r\n" + 
//      "    \"channels\": [{ \"name\": \"heartbeat\", \"product_ids\": [\"ETH-EUR\"] }]\r\n" + 
//      "}";
  
  
  public Socket(Source source,String URI) {
    LOG.info("socket created");
    this.queue = new LinkedBlockingQueue<>();
    this.source = source;
    this.url = URI;
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    LOG.info("Connection closed: {} - %s{}", statusCode, reason);
    try {
      this.start();
    } catch (IOException e) {
      LOG.error("Failure to restart web socket {}", e.getMessage());
    }
  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    LOG.info("Got connect: %s%n", session);
    
//    Enable this if you want to send a subscribe message
//    try {
//      Future<Void> fut;
//      fut = session.getRemote().sendStringByFuture(COINBASE_SUBSCRIBE_MESSAGE);
//      fut.get(1, TimeUnit.SECONDS);
//    } catch (Throwable t) {
//      t.printStackTrace();
//    }
  }

  @OnWebSocketMessage
  public void onMessage(String msg) {
    LOG.debug("Got msg ", msg);
    queue.offer(msg);
  }

  @Override
  public boolean start() throws IOException {
//	  Required 
    WebSocketClient client = new WebSocketClient(new SslContextFactory());
    try {
      LOG.info("Connecting to the Websocket");
      client.start();
      URI echoUri = new URI(url);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      client.connect(this, echoUri, request);
      LOG.info("Done Connecting");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    return advance();
  }

  @Override
//  Required
  public boolean advance() throws IOException {
    current = queue.poll();
    currentTimestamp = Instant.now();
    return (current != null);
  }

  @Override
//  Required
  public String getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
//  Required
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return currentTimestamp;
  }

  @Override
//  Required
  public void close() throws IOException {}

  @Override
//  Required
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    return new byte[0];
  }

  @Override
//  Required
  public Instant getWatermark() {
    return currentTimestamp.minus(new Duration(1));
  }

  @Override
//  Required
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return new UnboundedSource.CheckpointMark() {
      @Override
      public void finalizeCheckpoint() throws IOException {}
    };
  }

  @Override
//  Required
  public Source getCurrentSource() {
    return source;
  }
}
