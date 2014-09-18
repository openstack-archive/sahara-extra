/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.auth;

/**
 * Class that represents authentication request to Openstack Keystone v3.
 * Contains basic authentication information.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class PasswordAuthenticationRequestV3 extends AuthenticationRequestV3 {
  /**
   * Credentials for login
   */
  private IdentityWrapper identity;

  public PasswordAuthenticationRequestV3(PasswordCredentialsV3 passwordCredentials) {
    this.identity = new IdentityWrapper(new PasswordWrapper(passwordCredentials));
  }

  public IdentityWrapper getIdentity() {
    return identity;
  }

  public void setIdentity(IdentityWrapper identity) {
    this.identity = identity;
  }

  @Override
  public String toString() {
    return "Authenticate(v3) as " + identity.getPassword().getUser();
  }

  public static class IdentityWrapper {
    private PasswordWrapper password;
    public final String[] methods;

    public IdentityWrapper(PasswordWrapper password) {
      this.password = password;
      this.methods = new String[]{"password"};
    }

    public PasswordWrapper getPassword() {
      return password;
    }

    public void setPassword(PasswordWrapper password) {
      this.password = password;
    }
  }

  public static class PasswordWrapper {
    private PasswordCredentialsV3 user;

    public PasswordWrapper(PasswordCredentialsV3 user) {
      this.user = user;
    }

    public PasswordCredentialsV3 getUser() {
      return user;
    }

    public void setUser(PasswordCredentialsV3 user) {
      this.user = user;
    }
  }
}
