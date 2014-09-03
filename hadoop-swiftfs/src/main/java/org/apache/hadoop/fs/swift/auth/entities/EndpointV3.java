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

import java.net.URI;

/**
 * Openstack Swift endpoint description.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EndpointV3 {

  /**
   * endpoint id
   */
  private String id;

  /**
   * Keystone URL
   */
  private URI url;

  /**
   * Openstack region name
   */
  private String region;

  /**
   * Keystone URL type
   */
  private String iface;

  /**
   * @return endpoint id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id endpoint id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return Keystone URL
   */
  public URI getUrl() {
    return url;
  }

  /**
   * @param adminURL Keystone admin URL
   */
  public void setUrl(URI url) {
    this.url = url;
  }

  /**
   * @return Openstack region name
   */
  public String getRegion() {
    return region;
  }

  /**
   * @param region Openstack region name
   */
  public void setRegion(String region) {
    this.region = region;
  }

  public String getInterface() {
    return iface;
  }

  public void setInterface(String iface) {
    this.iface = iface;
  }
}
