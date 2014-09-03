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

package org.apache.hadoop.fs.swift.auth.entities;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.List;

/**
 * Describes Openstack Swift REST endpoints.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogV3 {
  /**
   * List of valid swift endpoints
   */
  private List<EndpointV3> endpoints;

  /**
   * Openstack REST service name. In our case name = "keystone"
   */
  private String name;

  /**
   * Type of REST service. In our case type = "identity"
   */
  private String type;

  /**
   * @return List of endpoints
   */
  public List<EndpointV3> getEndpoints() {
    return endpoints;
  }

  /**
   * @param endpoints list of endpoints
   */
  public void setEndpoints(List<EndpointV3> endpoints) {
    this.endpoints = endpoints;
  }

  /**
   * @return name of Openstack REST service
   */
  public String getName() {
    return name;
  }

  /**
   * @param name of Openstack REST service
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return type of Openstack REST service
   */
  public String getType() {
    return type;
  }

  /**
   * @param type of REST service
   */
  public void setType(String type) {
    this.type = type;
  }
}
