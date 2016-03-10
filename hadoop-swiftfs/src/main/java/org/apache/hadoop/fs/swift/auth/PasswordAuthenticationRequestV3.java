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

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonWriteNullProperties;

/**
 * Class that represents authentication request to Openstack Keystone v3.
 * Contains basic authentication information.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonWriteNullProperties(false)
public class PasswordAuthenticationRequestV3 extends AuthenticationRequestV3 {
  /**
   * Credentials for login
   */
  private final IdentityWrapper identity;
  private final ScopeWrapper scope;

  public PasswordAuthenticationRequestV3(ScopeWrapper scope,
                                         PasswordCredentialsV3 passwordCreds) {
    this.identity = new IdentityWrapper(new PasswordWrapper(passwordCreds));
    this.scope = scope;
  }

  public PasswordAuthenticationRequestV3(String projectName,
                                         PasswordCredentialsV3 passwordCreds) {
      this(projectName == null ? null :
           new ScopeWrapper(new ProjectWrapper(projectName, passwordCreds.domain)),
           passwordCreds);
  }

  public IdentityWrapper getIdentity() {
    return identity;
  }

  public ScopeWrapper getScope() {
    return scope;
  }

  @Override
  public String toString() {
    return "Authenticate(v3) as " + identity.getPassword().getUser();
  }

  public static class IdentityWrapper {
    private final PasswordWrapper password;
    private final String[] methods;

    public IdentityWrapper(PasswordWrapper password) {
      this.password = password;
      this.methods = new String[]{"password"};
    }

    public PasswordWrapper getPassword() {
      return password;
    }

    public String[] getMethods() {
      return methods;
    }
  }

  /**
   * THIS CLASS IS MAPPED BY JACKSON TO AND FROM JSON.
   * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
   */
  public static class PasswordWrapper {
    private final PasswordCredentialsV3 user;

    public PasswordWrapper(PasswordCredentialsV3 user) {
      this.user = user;
    }

    public PasswordCredentialsV3 getUser() {
      return user;
    }
  }

  /**
   * THIS CLASS IS MAPPED BY JACKSON TO AND FROM JSON.
   * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
   */
  @JsonWriteNullProperties(false)
  public static class ScopeWrapper {
    private final ProjectWrapper project;
    private final TrustWrapper trust;

    public ScopeWrapper(ProjectWrapper project) {
      this.project = project;
      this.trust = null;
    }

    public ScopeWrapper(TrustWrapper trust) {
      this.project = null;
      this.trust = trust;
    }

    public ProjectWrapper getProject() {
      return project;
    }

    @JsonProperty("OS-TRUST:trust")
    public TrustWrapper getTrust() {
      return trust;
    }
  }

  /**
   * THIS CLASS IS MAPPED BY JACKSON TO AND FROM JSON.
   * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
   */
  public static class ProjectWrapper {
    private final String name;
    private final Map<String, String> domain;

    public ProjectWrapper(String projectName, Map<String, String> domain) {
      this.domain = domain;
      this.name = projectName;
    }

    public String getName() {
      return name;
    }

    public Map<String, String> getDomain() {
      return domain;
    }
  }

  /**
   * THIS CLASS IS MAPPED BY JACKSON TO AND FROM JSON.
   * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
   */
  public static class TrustWrapper {
    private final String id;

    public TrustWrapper(String trustId) {
      id = trustId;
    }

    public String getId() {
      return id;
    }
  }
}
