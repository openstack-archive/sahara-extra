/**
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

package org.apache.hadoop.fs.swift.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * Describes credentials to log in Swift using Keystone v3 authentication.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class PasswordCredentialsV3 {
  /**
   * user login
   */
  private String name;

  /**
   * user password
   */
  private String password;

  /**
   * user's domain name
   */
  public final Map<String,String> domain;

  /**
   * @param name user login
   * @param password user password
   * @param domain_name user's domain name
   */
  public PasswordCredentialsV3(String name, String password, String domain_name, String domain_id) {
    this.name = name;
    this.password = password;
    this.domain = new HashMap();
    if (domain_id != null) {
      this.domain.put("id", domain_id);
    } else if (domain_name != null) {
      this.domain.put("name", domain_name);
    } else {
      this.domain.put("id", "default");
    }
  }

  /**
   * @return user password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password user password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return login
   */
  public String getName() {
    return name;
  }

  /**
   * @param username login
   */
  public void setName(String name) {
    this.name = name;
  }


  @Override
  public String toString() {
    String domain_info;
    if (domain.containsKey("id")) {
      domain_info = "domain id '" + domain.get("id") + "'";
    } else {
      domain_info = "domain name '" + domain.get("name") + "'";
    }
    return "user '" + name + '\'' +
            " with password of length " + ((password == null) ? 0 : password.length()) +
            ", " + domain_info;
  }
}
