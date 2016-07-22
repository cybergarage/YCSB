/*
 * Copyright (C) Satoshi Konno 2016
 *
 * This is licensed under BSD-style license, see file COPYING.
 */

package com.yahoo.ycsb.db;

import org.cybergarage.round.Node;
import org.cybergarage.round.Client;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import org.json.*;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.Iterator;
import java.net.InetAddress;

import java.net.URLEncoder;
import java.net.URLDecoder;

/**
 * Fractal client for YCSB framework.
 */
public class FractalClient extends DB {

  public static final String HOST_PROPERTY = "host";
  public static final String PORT_PROPERTY = "port";
  public static final String DEBUG_PROPERTY = "debug";
  public static final String METHODS_PROPERTY = "methods";

  public static final String DEFAULT_HOST = "127.0.0.1";
  public static final String DEFAULT_METHODS = "set_registry,get_registry,remove_registry";

  private boolean debug = false;
  private Client client = null;

  private String set_method;
  private String get_method;
  private String remove_method;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    try {
      debug = Boolean.parseBoolean(getProperties().getProperty(DEBUG_PROPERTY, "false"));
      String hosts[] = getProperties().getProperty(HOST_PROPERTY, DEFAULT_HOST).split(",");
      String port = getProperties().getProperty(PORT_PROPERTY, Integer.toString(Node.DEFAULT_PORT));

      String method[] = getProperties().getProperty(METHODS_PROPERTY, DEFAULT_METHODS).split(",");
      if (3 <= method.length) {
        set_method = method[0];
        get_method = method[1];
        remove_method = method[2];
      }

      if (debug) {
        System.out.println("host : ");
        int hostNo = 0;
        for (String host : hosts) {
            hostNo++;
            InetAddress addr = InetAddress.getByName(host); 
            System.out.println("  [" + Integer.toString(hostNo) + "] : " + host + " (" + addr.getHostAddress() + ")");
        }
        System.out.println("port : " + port);
        System.out.println("set_method : " + set_method);
        System.out.println("get_method : " + get_method);
        System.out.println("remove_method : " + remove_method);
      }

      this.client = new Client();
      for (String host : hosts) {
          InetAddress addr = InetAddress.getByName(host); 
          Node node = new Node(addr.getHostAddress(), Integer.valueOf(port));
          this.client.addNode(node);
      }

    } catch (Exception e) {}
  }
  
  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
  }
  
  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    String roundKey = "/" + table + "/" + key;
    Node node = this.client.getHandleNode(roundKey);
    
    if (!node.getRegistry(get_method, roundKey))
      return Status.ERROR;

    JSONObject resObj = node.getResponse();
    if (resObj == null)
      return Status.ERROR;

    if (debug) {
      System.out.println("read : " + roundKey + " " + resObj.toString());
    }

    try {
      JSONObject resultObj = (JSONObject)resObj.get("result");
      if (resultObj == null)
        return Status.ERROR;
      String values = resultObj.getString("val");
      if (values == null)
        return Status.ERROR;

      values = URLDecoder.decode(values, "UTF-8");
      /* Disabled Base64
      values = Base64.getEncoder().encodeToString(values.getBytes());
      */

      JSONObject fieldMap = new JSONObject(values);
      if (fieldMap != null) {
        Iterator<?> keyFields = fieldMap.keys();
        while (keyFields.hasNext()) {
          String keyField = (String) keyFields.next();
          result.put(keyField, null);
        }
      }
    } catch (Exception e) {
      return Status.ERROR;
    }

    return Status.OK;
  }
  
  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
  
  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
                       HashMap<String, ByteIterator> values) {
    // Insert and updates provide the same functionality
    return insert(table, key, values);
  }
  
  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
                       HashMap<String, ByteIterator> values) {
    String roundKey = "/" + table + "/" + key;
    Node node = this.client.getHandleNode(roundKey);

    JSONObject jsonObj = new JSONObject(values);
    String roundVal = jsonObj.toString();
    if (roundVal != null) {
      try {
        roundVal = URLEncoder.encode(roundVal, "UTF-8");
        /* Disabled Base64
        roundVal = new String(Base64.getDecoder().decode(roundVal));
        */
      } catch (Exception e) {}
    }

    if (debug) {
      System.out.println("insert : " + roundKey + " " + roundVal);
    }

    if (!node.setRegistry(set_method, roundKey, roundVal))
      return Status.ERROR;

    return Status.OK;
  }
  
  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    String roundKey = "/" + table + "/" + key;
    Node node = this.client.getHandleNode(roundKey);

    if (!node.removeRegistry(remove_method, roundKey))
      return Status.ERROR;

    return Status.OK;
  }

  public static void main(String[] args) {
  }
}
