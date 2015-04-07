/*
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

/**
 * Class that represents authentication request to Openstack Keystone v3.
 * Contains basic authentication information.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class TokenAuthenticationRequestV3 extends AuthenticationRequestV3 {
  /**
   * Credentials for login.
   */
  private final IdentityWrapper identity;

  public TokenAuthenticationRequestV3(String token) {
    this.identity = new IdentityWrapper(new TokenWrapper(token));
  }

  public IdentityWrapper getIdentity() {
    return identity;
  }

  @Override
  public String toString() {
    return "Authenticate(v3) as token";
  }

  /**
   * THIS CLASS IS MAPPED BY JACKSON TO AND FROM JSON.
   * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
   */
  public static class IdentityWrapper {
    private final TokenWrapper token;
    private final String[] methods;

    public IdentityWrapper(TokenWrapper token) {
      this.token = token;
      this.methods = new String[]{"token"};
    }

    public String[] getMethods() {
      return methods;
    }

    public TokenWrapper getToken() {
      return token;
    }
  }

  /**
   * THIS CLASS IS MAPPED BY JACKSON TO AND FROM JSON.
   * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
   */
  public static class TokenWrapper {
    private final String token;

    public TokenWrapper(String token) {
      this.token = token;
    }

    public String getId() {
      return token;
    }
  }
}
