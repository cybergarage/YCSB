/******************************************************************
*
* Round for Java
*
* Copyright (C) Satoshi Konno 2014
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

package org.cybergarage.round;

import org.junit.Assert;
import org.junit.Test;

import org.json.*;

/**
 * Integration tests for the Round node
 */
public class NodeTest
{
  @Test
  public void testId() throws Exception {
    Node node = new Node("127.0.0.1", Node.DEFAULT_PORT);
    String id = node.getId();
    Assert.assertNotNull(id);
    Assert.assertEquals(id.length(), Const.NODE_DIGEST_LENGTH);
  }

    @Test
    public void testSetRegistry() throws Exception {
        Node node = new Node("192.168.100.14", Node.DEFAULT_PORT);

        String key = "key";
        String value = "{\"test\":\"hello\"}";

        JSONObject resObj;

        Assert.assertTrue(node.setRegistry(key, value));
        resObj = node.getResponse();
        System.out.println(resObj.toString());

        Assert.assertTrue(node.getRegistry(key));
        resObj = node.getResponse();
        if (resObj != null) {
          System.out.println("resObj : " + resObj.toString());
          JSONObject resultObj = (JSONObject)resObj.get("result");
          if (resultObj != null) {
            System.out.println("resultObj : " + resultObj.toString());
            String keyValue = resultObj.getString("val");
            System.out.println("keyValue : " + keyValue);
          }
        }

        Assert.assertTrue(node.removeRegistry(key));
        resObj = node.getResponse();
        System.out.println(resObj.toString());
    }
}
