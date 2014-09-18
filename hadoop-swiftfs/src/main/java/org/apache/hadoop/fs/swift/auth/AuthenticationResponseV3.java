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

import org.apache.hadoop.fs.swift.auth.entities.CatalogV3;
import org.apache.hadoop.fs.swift.auth.entities.Tenant;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.List;

/**
 * Response from KeyStone deserialized into AuthenticationResponse class.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthenticationResponseV3 {
  private List<CatalogV3> catalog;
  private String expires_at;
  private Tenant project;

  public List<CatalogV3> getCatalog() {
    return catalog;
  }

  public void setCatalog(List<CatalogV3> catalog) {
    this.catalog = catalog;
  }

  public String getExpires_at() {
    return expires_at;
  }

  public void setExpires_at(String expires_at) {
    this.expires_at = expires_at;
  }

  public Tenant getProject() {
    return project;
  }

  public void setProject(Tenant project) {
    this.project = project;
  }

}
