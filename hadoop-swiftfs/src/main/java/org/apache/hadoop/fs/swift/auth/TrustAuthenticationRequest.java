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

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class that represents authentication request to Openstack Keystone v3.
 * Contains basic authentication information.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class TrustAuthenticationRequest extends PasswordAuthenticationRequestV3 {
  /**
   * trust-id for login
   */
  private ScopeWrapper scope;

  public TrustAuthenticationRequest(PasswordCredentialsV3 passwordCredentials, String trust_id) {
    super(passwordCredentials);
    scope = new ScopeWrapper(new TrustWrapper(trust_id));
  }

  public ScopeWrapper getScope() {
    return scope;
  }

  public void setScope(ScopeWrapper scope) {
    this.scope = scope;
  }

  @Override
  public String toString() {
    return super.toString() +
            ", trust-id '" + scope.getTrust().getId() + "'";
  }

  public static class ScopeWrapper {
    private TrustWrapper trust;

    public ScopeWrapper(TrustWrapper trust) {
      this.trust = trust;
    }

    @JsonProperty("OS-TRUST:trust")
    public TrustWrapper getTrust() {
      return trust;
    }

    @JsonProperty("OS-TRUST:trust")
    public void setTrust(TrustWrapper trust) {
      this.trust = trust;
    }
  }

  public static class TrustWrapper {
    private String id;

    public TrustWrapper(String trust_id) {
      id = trust_id;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }
  }
}
