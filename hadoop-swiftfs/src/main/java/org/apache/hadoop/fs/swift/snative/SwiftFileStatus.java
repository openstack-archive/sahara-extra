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

package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;

/**
 * A subclass of {@link FileStatus} that contains the
 * Swift-specific meta-data (e.g. DLO)
 */
public class SwiftFileStatus extends FileStatus {

  private SwiftObjectPath dloPrefix = null;

  private boolean isPseudoDirFlag = false;

  public SwiftFileStatus() {
  }

  public SwiftFileStatus(long length,
                         boolean isdir,
                         int block_replication,
                         long blocksize, long modification_time, Path path) {
    this(length, isdir, block_replication, blocksize, modification_time,
            path, null);
  }

  public SwiftFileStatus(long length,
                         boolean isdir,
                         int block_replication,
                         long blocksize, long modification_time, Path path,
                         SwiftObjectPath dloPrefix) {
    super(length, isdir, block_replication, blocksize, modification_time, path);
    this.dloPrefix = dloPrefix;
  }

  public SwiftFileStatus(long length,
                         boolean isdir,
                         int block_replication,
                         long blocksize,
                         long modification_time,
                         long access_time,
                         FsPermission permission,
                         String owner, String group, Path path) {
    super(length, isdir, block_replication, blocksize, modification_time,
            access_time, permission, owner, group, path);
  }

  public static SwiftFileStatus createPseudoDirStatus(Path path) {
    SwiftFileStatus status = new SwiftFileStatus(0, true, 1, 0,
                                                 System.currentTimeMillis(),
                                                 path);
    status.isPseudoDirFlag = true;
    return status;
  }

  /**
   * A entry is a file if it is not a directory.
   * By implementing it <i>and not marking as an override</i> this
   * subclass builds and runs in both Hadoop versions.
   * @return the opposite value to {@link #isDir()}
   */
  public boolean isFile() {
    return !isDir();
  }

  /**
   * Directory test
   * @return true if the file is considered to be a directory
   */
  public boolean isDirectory() {
    return isDir();
  }

  public boolean isDLO() {
    return dloPrefix != null;
  }

  public SwiftObjectPath getDLOPrefix() {
    return dloPrefix;
  }

  public boolean isPseudoDir() {
    return isPseudoDirFlag;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append("{ ");
    sb.append("path=").append(getPath());
    sb.append("; isDirectory=").append(isDirectory());
    sb.append("; length=").append(getLen());
    sb.append("; blocksize=").append(getBlockSize());
    sb.append("; modification_time=").append(getModificationTime());
    sb.append("}");
    return sb.toString();
  }
}
