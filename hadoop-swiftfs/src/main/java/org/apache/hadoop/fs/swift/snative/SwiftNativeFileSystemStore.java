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
package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftInvalidResponseException;
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.http.HttpBodyContent;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.util.DurationStats;
import org.apache.hadoop.fs.swift.util.JSONUtil;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.map.type.CollectionType;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File system store implementation.
 * Makes REST requests, parses data from responses
 */
public class SwiftNativeFileSystemStore {
  private static final Pattern URI_PATTERN = Pattern.compile("\"\\S+?\"");
  private static final String PATTERN = "EEE, d MMM yyyy hh:mm:ss zzz";
  private static final Log LOG =
          LogFactory.getLog(SwiftNativeFileSystemStore.class);
  private URI uri;
  private SwiftRestClient swiftRestClient;
  private DNSToSwitchMapping dnsToSwitchMapping;

  /**
   * Initialize the filesystem store -this creates the REST client binding.
   *
   * @param fsURI         URI of the filesystem, which is used to map to the filesystem-specific
   *                      options in the configuration file
   * @param configuration configuration
   * @throws IOException on any failure.
   */
  public void initialize(URI fsURI, Configuration configuration) throws IOException {
    this.uri = fsURI;
    dnsToSwitchMapping = ReflectionUtils.newInstance(
        configuration.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), configuration);

    this.swiftRestClient = SwiftRestClient.getInstance(fsURI, configuration);
  }

  @Override
  public String toString() {
    return "SwiftNativeFileSystemStore with "
            + swiftRestClient;
  }

  /**
   * Get the default blocksize of this (bound) filesystem
   * @return the blocksize returned for all FileStatus queries,
   * which is used by the MapReduce splitter.
   */
  public long getBlocksize() {
    return 1024L * swiftRestClient.getBlocksizeKB();
  }

  public long getPartsizeKB() {
    return swiftRestClient.getPartSizeKB();
  }

  public int getBufferSizeKB() {
    return swiftRestClient.getBufferSizeKB();
  }

  public int getThrottleDelay() {
    return swiftRestClient.getThrottleDelay();
  }
  /**
   * Upload a file/input stream of a specific length.
   *
   * @param path        destination path in the swift filesystem
   * @param inputStream input data. This is closed afterwards, always
   * @param length      length of the data
   * @throws IOException on a problem
   */
  public void uploadFile(Path path, InputStream inputStream, long length)
          throws IOException {
      swiftRestClient.upload(toObjectPath(path), inputStream, length);
  }

  /**
   * Upload part of a larger file.
   *
   * @param path        destination path
   * @param partNumber  item number in the path
   * @param inputStream input data
   * @param length      length of the data
   * @throws IOException on a problem
   */
  public void uploadFilePart(Path path, int partNumber,
                             InputStream inputStream, long length)
          throws IOException {

    String stringPath = path.toUri().getPath();
    String partitionFilename = SwiftUtils.partitionFilenameFromNumber(
      partNumber);
    if (stringPath.endsWith("/")) {
      stringPath = stringPath.concat(partitionFilename);
    } else {
      stringPath = stringPath.concat("/").concat(partitionFilename);
    }

    swiftRestClient.upload(
      new SwiftObjectPath(toDirPath(path).getContainer(), stringPath),
            inputStream,
            length);
  }

  /**
   * Tell the Swift server to expect a multi-part upload by submitting
   * a 0-byte file with the X-Object-Manifest header
   *
   * @param path path of final final
   * @throws IOException
   */
  public void createManifestForPartUpload(Path path) throws IOException {
    String pathString = toObjectPath(path).toString();
    if (!pathString.endsWith("/")) {
      pathString = pathString.concat("/");
    }
    if (pathString.startsWith("/")) {
      pathString = pathString.substring(1);
    }

    swiftRestClient.upload(toObjectPath(path),
            new ByteArrayInputStream(new byte[0]),
            0,
            new Header(SwiftProtocolConstants.X_OBJECT_MANIFEST, pathString));
  }

  /**
   * Get the metadata of an object
   *
   * @param path path
   * @return file metadata. -or null if no headers were received back from the server.
   * @throws IOException           on a problem
   * @throws FileNotFoundException if there is nothing at the end
   */
  public SwiftFileStatus getObjectMetadata(Path path) throws IOException {
    return getObjectMetadata(path, true);
  }

  /**
   * Get the HTTP headers, in case you really need the low-level
   * metadata
   * @param path path to probe
   * @param newest newest or oldest?
   * @return the header list
   * @throws IOException IO problem
   * @throws FileNotFoundException if there is nothing at the end
   */
  public Header[] getObjectHeaders(Path path, boolean newest)
    throws IOException, FileNotFoundException {
    SwiftObjectPath objectPath = toObjectPath(path);
    return stat(objectPath, newest);
  }

  /**
   * Get the metadata of an object
   *
   * @param path path
   * @param newest flag to say "set the newest header", otherwise take any entry
   * @return file metadata. -or null if no headers were received back from the server.
   * @throws IOException           on a problem
   * @throws FileNotFoundException if there is nothing at the end
   */
  public SwiftFileStatus getObjectMetadata(Path path, boolean newest)
    throws IOException, FileNotFoundException {

    SwiftObjectPath objectPath = toObjectPath(path);

    // remove trailing slash because FileStatus must not include that
    Path statusPath = path;
    if (statusPath.toUri().toString().endsWith("/")) {
      String pathUri = statusPath.toUri().toString();
      if (pathUri.length() > 1)
        statusPath = new Path(pathUri.substring(0, pathUri.length() - 1));
    }

    Header[] headers = null;
    try {
      headers = stat(objectPath, newest);
    } catch (FileNotFoundException e) {
      try {
        // retry stat for direcotry file
        objectPath = toDirPath(path);
        headers = stat(objectPath, newest);
      } catch (FileNotFoundException ex) {
        // if path is pseudo-directory, ignore FileNotFoundException.
      }
    }
    //no headers is treated as a missing file or pseudo-directory
    if (headers == null || headers.length == 0) {
      if (existsPseudoDirectory(objectPath)) {
        Path pseudoDirPath = getCorrectSwiftPath(statusPath);
        return SwiftFileStatus.createPseudoDirStatus(pseudoDirPath);
      }
      throw new FileNotFoundException("Not Found " + path.toUri());
    }

    boolean isDir = false;
    long length = 0;
    long lastModified = 0 ;
    SwiftObjectPath dloPrefix = null;
    for (Header header : headers) {
      String headerName = header.getName();
      if (headerName.equals(SwiftProtocolConstants.X_CONTAINER_OBJECT_COUNT) ||
              headerName.equals(SwiftProtocolConstants.X_CONTAINER_BYTES_USED)) {
        length = 0;
        isDir = true;
      }
      if (SwiftProtocolConstants.HEADER_CONTENT_LENGTH.equals(headerName)) {
        length = Long.parseLong(header.getValue());
      }
      if (SwiftProtocolConstants.HEADER_LAST_MODIFIED.equals(headerName)) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN, Locale.US);
        try {
          lastModified = simpleDateFormat.parse(header.getValue()).getTime();
        } catch (ParseException e) {
          throw new SwiftException("Failed to parse " + header.toString(), e);
        }
      }
      if (headerName.equals(SwiftProtocolConstants.X_OBJECT_MANIFEST)) {
        String[] values = header.getValue().split("/", 2);
        if (values.length == 2) {
          dloPrefix = new SwiftObjectPath(values[0], "/" + values[1]);
        }
      }
    }
    if (lastModified == 0) {
      lastModified = System.currentTimeMillis();
    }
    if (objectPath.toString().endsWith("/")) {
      isDir = true;
    }

    Path correctSwiftPath = getCorrectSwiftPath(statusPath);
    return new SwiftFileStatus(length,
                               isDir,
                               1,
                               getBlocksize(),
                               lastModified,
                               correctSwiftPath,
                               dloPrefix);
  }

  private Header[] stat(SwiftObjectPath objectPath, boolean newest) throws
                                                                    IOException {
    Header[] headers;
    if (newest) {
      headers = swiftRestClient.headRequest("getObjectMetadata-newest",
                                            objectPath, SwiftRestClient.NEWEST);
    } else {
      headers = swiftRestClient.headRequest("getObjectMetadata",
                                            objectPath);
    }
    return headers;
  }

  private boolean existsPseudoDirectory(SwiftObjectPath path) {
    try {
      String pseudoDirName = path.getObject();
      if (pseudoDirName.endsWith("/")) {
        String obj = path.getObject();
        path = new SwiftObjectPath(path.getContainer(),
                                   obj.substring(0, obj.length() - 1));
      } else {
        pseudoDirName = pseudoDirName.concat("/");
      }

      final byte[] bytes;
      bytes = swiftRestClient.listDeepObjectsInDirectory(path, false, false);

      final CollectionType collectionType = JSONUtil.getJsonMapper().
        getTypeFactory().constructCollectionType(List.class,
                                                 SwiftObjectFileStatus.class);

      final List<SwiftObjectFileStatus> fileStatusList =
        JSONUtil.toObject(new String(bytes), collectionType);

      for (SwiftObjectFileStatus status : fileStatusList) {
        if (pseudoDirName.equals(status.getSubdir())) {
          return true;
        }
      }
      return false;

    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Get the object as an input stream
   *
   * @param path object path
   * @return the input stream -this must be closed to terminate the connection
   * @throws IOException           IO problems
   * @throws FileNotFoundException path doesn't resolve to an object
   */
  public HttpBodyContent getObject(Path path) throws IOException {
    List<String> locations = getDataLocalEndpoints(path);

    for (String url : locations) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading " + path + " from location: " + url);
      }
      try {
        return swiftRestClient.getData(new URI(url),
            SwiftRestClient.NEWEST);
      } catch (Exception e) {
        // Ignore
        // It is possible that endpoint doesn't contains needed data.
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reading " + path + " from proxy node");
    }
    return swiftRestClient.getData(toObjectPath(path),
                                 SwiftRestClient.NEWEST);
  }

  /**
   * Returns list of endpoints for given swift path that are local for the
   * host. List is returned in order of preference.
   */
  private List<String> getDataLocalEndpoints(Path path) throws IOException {
    final String hostRack = getHostRack();

    List<URI> uriLocations = getObjectLocation(path);
    List<String> strLocations = new ArrayList<String>();
    final Map<String, Integer> similarityMap = new HashMap<String, Integer>();
    for (URI uri : uriLocations) {
      String url = uri.toString();
      int similarity = getSimilarity(getRack(uri.getHost()), hostRack);
      if (similarity > 0) {
        strLocations.add(url);
        similarityMap.put(url, similarity);
      }
    }

    Collections.sort(strLocations, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        Integer dst1 = similarityMap.get(o1);
        Integer dst2 = similarityMap.get(o2);
        return -dst1.compareTo(dst2);
      }
    });

    return strLocations;
  }

  /**
   * Returns similarity index for two racks.
   * Bigger numbers correspond to closer location.
   * Zero corresponds to different racks.
   *
   * @param rack1 path to rack1
   * @param rack2 path to rack2
   * @return the similarity index
   */
  private int getSimilarity(String rack1, String rack2) {
    String[] r1 = rack1.split("/");
    String[] r2 = rack2.split("/");
    int i = 1; //skip leading empty string
    while (i < r1.length && i < r2.length && r1[i].equals(r2[i])) {
      i++;
    }

    return i - 1;
  }

  private String getHostRack() throws SwiftException {
    String hostAddress;
    try {
      hostAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new SwiftException("Failed to get localhost address", e);
    }
    return getRack(hostAddress);
  }

  private String getRack(String url) {
    return dnsToSwitchMapping.resolve(Arrays.asList(url)).get(0);
  }

  /**
   * Get the input stream starting from a specific point.
   *
   * @param path           path to object
   * @param byteRangeStart starting point
   * @param length         no. of bytes
   * @return an input stream that must be closed
   * @throws IOException IO problems
   */
  public HttpBodyContent getObject(Path path, long byteRangeStart, long length)
          throws IOException {
    List<String> locations = getDataLocalEndpoints(path);

    for (String url : locations) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reading " + path + " from location: " + url);
      }
      try {
        return swiftRestClient.getData(new URI(url), byteRangeStart, length);
      } catch (Exception e) {
        //Ignore
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reading " + path + " from proxy node");
    }

    return swiftRestClient.getData(
      toObjectPath(path), byteRangeStart, length);
  }

  /**
   * List a directory.
   * This is O(n) for the number of objects in this path.
   *
   *
   *
   * @param path working path
   * @param listDeep ask for all the data
   * @param newest ask for the newest data
   * @param addTrailingSlash should a trailing slash be added if there isn't one
   * @return Collection of file statuses
   * @throws IOException IO problems
   * @throws FileNotFoundException if the path does not exist
   */
  private List<FileStatus> listDirectory(SwiftObjectPath path,
                                         boolean listDeep,
                                         boolean newest,
                                         boolean addTrailingSlash)
      throws IOException {
    final byte[] bytes;
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();
    final Path correctSwiftPath = getCorrectSwiftPath(path);
    try {
      bytes = swiftRestClient.listDeepObjectsInDirectory(path, listDeep,
                                                         addTrailingSlash);
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("" +
                "File/Directory not found " + path);
      }
      if (SwiftUtils.isRootDir(path)) {
        return Collections.emptyList();
      } else {
        throw e;
      }
    } catch (SwiftInvalidResponseException e) {
      //bad HTTP error code
      if (e.getStatusCode() == HttpStatus.SC_NO_CONTENT) {
        //this can come back on a root list if the container is empty
        if (SwiftUtils.isRootDir(path)) {
          return Collections.emptyList();
        } else {
          //NO_CONTENT returned on something other than the root directory;
          //see if it is there, and convert to empty list or not found
          //depending on whether the entry exists.
          FileStatus stat = getObjectMetadata(correctSwiftPath, newest);

          if (stat.isDir()) {
            //it's an empty directory. state that
            return Collections.emptyList();
          } else {
            //it's a file -return that as the status
            files.add(stat);
            return files;
          }
        }
      } else {
        //a different status code: rethrow immediately
        throw e;
      }
    }

    final CollectionType collectionType = JSONUtil.getJsonMapper().getTypeFactory().
            constructCollectionType(List.class, SwiftObjectFileStatus.class);

    final List<SwiftObjectFileStatus> fileStatusList =
            JSONUtil.toObject(new String(bytes), collectionType);

    //this can happen if user lists file /data/files/file
    //in this case swift will return empty array
    if (fileStatusList.isEmpty()) {
      SwiftFileStatus objectMetadata = getObjectMetadata(correctSwiftPath,
                                                         newest);
      if (objectMetadata.isFile()) {
        files.add(objectMetadata);
      }

      return files;
    }

    String pathWithSlash = path.getObject();
    if (!pathWithSlash.endsWith("/")) {
      pathWithSlash = pathWithSlash.concat("/");
    }
    String prevObjName = "";
    for (SwiftObjectFileStatus status : fileStatusList) {
      String name = status.getName();
      if (name == null) {
        name = status.getSubdir();
      }
      if (name == null || name.equals(pathWithSlash)) {
        continue;
      }

      if (!name.endsWith("/")) {
        final Path filePath = getCorrectSwiftPath(new Path(name));
        files.add(getObjectMetadata(filePath, newest));
        prevObjName = name;
      } else {
        if (prevObjName.length() + 1 == name.length() &&
            name.startsWith(prevObjName)) {
          // Ignore because this directory object is stored DLO segment file
          continue;
        }
        final Path dirPath = getCorrectSwiftPath(toDirPath(new Path(name)));
        files.add(getObjectMetadata(dirPath, newest));
      }
    }

    return files;
  }

  /**
   * List all elements in this directory
   *
   *
   *
   * @param path     path to work with
   * @param recursive do a recursive get
   * @param newest ask for the newest, or can some out of date data work?
   * @return the file statuses, or an empty array if there are no children
   * @throws IOException           on IO problems
   * @throws FileNotFoundException if the path is nonexistent
   */
  public FileStatus[] listSubPaths(Path path,
                                   boolean recursive,
                                   boolean newest) throws IOException {
    final Collection<FileStatus> fileStatuses;
    fileStatuses = listDirectory(toDirPath(path), recursive, newest, true);
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  /**
   * Create a directory
   *
   * @param path path
   * @throws IOException
   */
  public void createDirectory(Path path) throws IOException {
    innerCreateDirectory(toDirPath(path));
  }

  /**
   * The inner directory creation option. This only creates
   * the dir at the given path, not any parent dirs.
   * @param swiftObjectPath swift object path at which a 0-byte blob should be
   * put
   * @throws IOException IO problems
   */
  private void innerCreateDirectory(SwiftObjectPath swiftObjectPath)
          throws IOException {

    swiftRestClient.putRequest(swiftObjectPath);
  }

  private SwiftObjectPath toDirPath(Path path) throws
          SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path, true);
  }

  private SwiftObjectPath toObjectPath(Path path) throws
          SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path);
  }

  /**
   * Try to find the specific server(s) on which the data lives
   * @param path path to probe
   * @return a possibly empty list of locations
   * @throws IOException on problems determining the locations
   */
  public List<URI> getObjectLocation(Path path) throws IOException {
    final byte[] objectLocation;
    objectLocation = swiftRestClient.getObjectLocation(toObjectPath(path));
    if (objectLocation == null || objectLocation.length == 0) {
      //no object location, return an empty list
      return new LinkedList<URI>();
    }
    return extractUris(new String(objectLocation), path);
  }

  /**
   * deletes object from Swift
   *
   * @param status FileStatus to delete
   * @return true if the path was deleted by this specific operation.
   * @throws IOException on a failure
   */
  public boolean deleteObject(FileStatus status) throws IOException {
    SwiftObjectPath swiftObjectPath;
    if (status.isDir()) {
      swiftObjectPath = toDirPath(status.getPath());
    } else {
      swiftObjectPath = toObjectPath(status.getPath());
    }
    if (!SwiftUtils.isRootDir(swiftObjectPath)) {
      return swiftRestClient.delete(swiftObjectPath);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not deleting root directory entry");
      }
      return true;
    }
  }

  /**
   * Does the object exist
   *
   * @param path object path
   * @return true if the metadata of an object could be retrieved
   * @throws IOException IO problems other than FileNotFound, which
   *                     is downgraded to an object does not exist return code
   */
  public boolean objectExists(Path path) throws IOException {
    return objectExists(toObjectPath(path));
  }

  /**
   * Does the object exist
   *
   * @param path swift object path
   * @return true if the metadata of an object could be retrieved
   * @throws IOException IO problems other than FileNotFound, which
   *                     is downgraded to an object does not exist return code
   */
  public boolean objectExists(SwiftObjectPath path) throws IOException {
    try {
      Header[] headers = swiftRestClient.headRequest("objectExists",
                                                     path,
                                                     SwiftRestClient.NEWEST);
      //no headers is treated as a missing file
      return headers.length != 0;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Rename through copy-and-delete. this is a consequence of the
   * Swift filesystem using the path as the hash
   * into the Distributed Hash Table, "the ring" of filenames.
   * <p/>
   * Because of the nature of the operation, it is not atomic.
   *
   * @param src source file/dir
   * @param dst destination
   * @throws IOException                   IO failure
   * @throws SwiftOperationFailedException if the rename failed
   * @throws FileNotFoundException         if the source directory is missing, or
   *                                       the parent directory of the destination
   */
  public void rename(Path src, Path dst)
    throws FileNotFoundException, SwiftOperationFailedException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("mv " + src + " " + dst);
    }
    boolean renamingOnToSelf = src.equals(dst);

    final SwiftFileStatus srcMetadata;
    srcMetadata = getObjectMetadata(src);
    SwiftObjectPath srcObject;
    if (srcMetadata.isDirectory()) {
      srcObject = toDirPath(src);
    } else {
      srcObject = toObjectPath(src);
    }
    if (SwiftUtils.isRootDir(srcObject)) {
      throw new SwiftOperationFailedException("cannot rename root dir");
    }

    SwiftFileStatus dstMetadata;
    SwiftObjectPath destObject;
    try {
      dstMetadata = getObjectMetadata(dst);
      if (dstMetadata.isDirectory()) {
        destObject = toDirPath(dst);
      } else {
        destObject = toObjectPath(dst);
      }
    } catch (FileNotFoundException e) {
      //destination does not exist.
      LOG.debug("Destination does not exist");
      dstMetadata = null;
      destObject = toObjectPath(dst);
    }

    //check to see if the destination parent directory exists
    Path srcParent = src.getParent();
    Path dstParent = dst.getParent();
    //skip the overhead of a HEAD call if the src and dest share the same
    //parent dir (in which case the dest dir exists), or the destination
    //directory is root, in which case it must also exist
    if (dstParent != null && !dstParent.equals(srcParent)) {
      try {
        getObjectMetadata(dstParent);
      } catch (FileNotFoundException e) {
        //destination parent doesn't exist; bail out
        LOG.debug("destination parent directory " + dstParent + " doesn't exist");
        throw e;
      }
    }

    boolean destExists = dstMetadata != null;
    boolean destIsDir = destExists && dstMetadata.isDirectory();
    //calculate the destination
    SwiftObjectPath destPath;

    if (srcMetadata.isFile()) {

      //source is a simple file OR a partitioned file
      // outcomes:
      // #1 dest exists and is file: fail
      // #2 dest exists and is dir: destination path becomes under dest dir
      // #3 dest does not exist: use dest as name
      if (destExists) {

        if (destIsDir) {
          //outcome #2 -move to subdir of dest
          destPath = toObjectPath(new Path(dst, src.getName()));
        } else {
          //outcome #1 dest it's a file: fail if differeent
          if (!renamingOnToSelf) {
            throw new SwiftOperationFailedException(
                    "cannot rename a file over one that already exists");
          } else {
            //is mv self self where self is a file. this becomes a no-op
            LOG.debug("Renaming file onto self: no-op => success");
            return;
          }
        }
      } else {
        //outcome #3 -new entry
        destPath = toObjectPath(dst);
      }

      copyThenDeleteObject(srcObject, srcMetadata, destPath);
    } else {

      //here the source exists and is a directory
      // outcomes (given we know the parent dir exists if we get this far)
      // #1 destination is a file: fail
      // #2 destination is a directory: create a new dir under that one
      // #3 destination doesn't exist: create a new dir with that name
      // #3 and #4 are only allowed if the dest path is not == or under src


      if (destExists && !destIsDir) {
        // #1 destination is a file: fail
        throw new SwiftOperationFailedException(
                "the source is a directory, but not the destination");
      }
      Path targetPath;
      if (destExists) {
        // #2 destination is a directory: create a new dir under that one
        targetPath = new Path(dst, src.getName());
      } else {
        // #3 destination doesn't exist: create a new dir with that name
        targetPath = dst;
      }
      SwiftObjectPath targetObjectPath = toDirPath(targetPath);
      //final check for any recursive operations
      if (srcObject.isEqualToOrParentOf(targetObjectPath)) {
        //you can't rename a directory onto itself
        throw new SwiftOperationFailedException(
          "cannot move a directory under itself");
      }


      LOG.info("mv  " + srcObject + " " + targetPath);

      //enum the child entries and everything underneath
      List<FileStatus> childStats = listDirectory(srcObject, true, true, true);
      logDirectory("Directory to copy ", srcObject, childStats);

      // iterative copy of everything under the directory.
      // by listing all children this can be done iteratively
      // rather than recursively -everything in this list is either a file
      // or a 0-byte-len file pretending to be a directory.
      String srcURI = src.toUri().toString();
      int prefixStripCount = srcURI.length() + 1;
      for (FileStatus fileStatus : childStats) {
        Path copySourcePath = fileStatus.getPath();
        String copySourceURI = copySourcePath.toUri().toString();

        String copyDestSubPath = copySourceURI.substring(prefixStripCount);

        Path copyDestPath = new Path(targetPath, copyDestSubPath);
        if (LOG.isTraceEnabled()) {
          //trace to debug some low-level rename path problems; retained
          //in case they ever come back.
          LOG.trace("srcURI=" + srcURI
                  + "; copySourceURI=" + copySourceURI
                  + "; copyDestSubPath=" + copyDestSubPath
                  + "; copyDestPath=" + copyDestPath);
        }
        SwiftObjectPath copySource;
        SwiftObjectPath copyDestination;

        if (fileStatus.isDir()) {
          copySource = toDirPath(copySourcePath);
          copyDestination = toDirPath(copyDestPath);
        } else {
          copySource = toObjectPath(copySourcePath);
          copyDestination = toObjectPath(copyDestPath);
        }

        try {
          copyThenDeleteObject(copySource, (SwiftFileStatus)fileStatus,
                               copyDestination);
        } catch (FileNotFoundException e) {
          LOG.info("Skipping rename of " + copySourcePath);
        }
        //add a throttle delay
        throttle();
      }
      //now rename self. If missing, create the dest directory and warn
      if (!SwiftUtils.isRootDir(srcObject)) {
        try {
          if (!srcMetadata.isPseudoDir()) {
            copyThenDeleteObject(srcObject, srcMetadata, targetObjectPath);
          }
        } catch (FileNotFoundException e) {
          //create the destination directory
          LOG.warn("Source directory deleted during rename", e);
          innerCreateDirectory(destObject);
        }
      }
    }
  }

  /**
   * Debug action to dump directory statuses to the debug log
   *
   * @param message    explanation
   * @param objectPath object path (can be null)
   * @param statuses   listing output
   */
  private void logDirectory(String message, SwiftObjectPath objectPath,
                            Iterable<FileStatus> statuses) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(message + ": listing of " + objectPath);
      for (FileStatus fileStatus : statuses) {
        LOG.debug(fileStatus.getPath());
      }
    }
  }

  /**
   * Copy an object then, if the copy worked, delete it.
   * If the copy failed, the source object is not deleted.
   *
   * @param srcObject  source object path
   * @param destObject destination object path
   * @throws IOException IO problems

   */
  private void copyThenDeleteObject(SwiftObjectPath srcObject,
                                    SwiftFileStatus srcMeta,
                                    SwiftObjectPath destObject) throws
          IOException {

    //do the copy
    copyObject(srcObject, srcMeta, destObject, true);
  }
  /**
   * Copy an object
   * @param srcObject  source object path
   * @param srcMeta    source object status
   * @param destObject destination object path
   * @param deleteSrc  if true and the copy worked, delete source object
   * @throws IOException IO problems
   */
  private void copyObject(SwiftObjectPath srcObject,
                          SwiftFileStatus srcMeta,
                          SwiftObjectPath destObject,
                          boolean deleteSrc) throws
          IOException {
    if (srcObject.isEqualToOrParentOf(destObject)) {
      throw new SwiftException(
        "Can't copy " + srcObject + " onto " + destObject);
    }
    if (srcMeta.isDLO()) {
      Path newPrefixPath = getCorrectSwiftPath(destObject);
      String newPrefixName = newPrefixPath.toUri().getPath();
      if (!newPrefixName.endsWith("/")) {
        newPrefixName = newPrefixName.concat("/");
      }
      String oldPrefixName = srcMeta.getDLOPrefix().getObject();
      List<FileStatus> segments = listDirectory(srcMeta.getDLOPrefix(),
                                                true, true, true);
      createManifestForPartUpload(newPrefixPath);
      for (FileStatus s : segments) {
        if (s.getLen() == 0) {
          continue;
        }
        String oldName = s.getPath().toUri().getPath();
        String newName = newPrefixName
          + oldName.substring(oldPrefixName.length());
        SwiftObjectPath srcSeg = new SwiftObjectPath(srcObject.getContainer(),
                                                     oldName);
        SwiftObjectPath destSeg = new SwiftObjectPath(destObject.getContainer(),
                                                      newName);
        if (!swiftRestClient.copyObject(srcSeg, destSeg)) {
          throw new SwiftException("Copy of " + srcSeg + " to "
                                 + destSeg + "failed");
        }
      }
      if (deleteSrc) {
        for (FileStatus s : segments) {
          SwiftObjectPath srcSeg =
            new SwiftObjectPath(srcObject.getContainer(),
                                s.getPath().toUri().getPath());
          swiftRestClient.delete(srcSeg);
        }
      }
    } else {
      //do the copy
      boolean copySucceeded = swiftRestClient.copyObject(srcObject, destObject);
      if (!copySucceeded) {
        throw new SwiftException("Copy of " + srcObject + " to "
                                 + destObject + "failed");
      }
    }
    if (deleteSrc) {
      swiftRestClient.delete(srcObject);
    }
  }

  /**
   * Take a Hadoop path and return one which uses the URI prefix and authority
   * of this FS. It doesn't make a relative path absolute
   * @param path path in
   * @return path with a URI bound to this FS
   * @throws SwiftException URI cannot be created.
   */
  public Path getCorrectSwiftPath(Path path) throws
          SwiftException {
    try {
      final URI fullUri = new URI(uri.getScheme(),
              uri.getAuthority(),
              path.toUri().getPath(),
              null,
              null);

      return new Path(fullUri);
    } catch (URISyntaxException e) {
      throw new SwiftException("Specified path " + path + " is incorrect", e);
    }
  }

  /**
   * Builds a hadoop-Path from a swift path, inserting the URI authority
   * of this FS instance
   * @param path swift object path
   * @return Hadoop path
   * @throws SwiftException if the URI couldn't be created.
   */
  private Path getCorrectSwiftPath(SwiftObjectPath path) throws
          SwiftException {
    try {
      final URI fullUri = new URI(uri.getScheme(),
              uri.getAuthority(),
              path.getObject(),
              null,
              null);

      return new Path(fullUri);
    } catch (URISyntaxException e) {
      throw new SwiftException("Specified path " + path + " is incorrect", e);
    }
  }


  /**
   * extracts URIs from json
   * @param json json to parse
   * @param path path (used in exceptions)
   * @return URIs
   * @throws SwiftOperationFailedException on any problem parsing the JSON
   */
  public static List<URI> extractUris(String json, Path path) throws
                                                   SwiftOperationFailedException {
    final Matcher matcher = URI_PATTERN.matcher(json);
    final List<URI> result = new ArrayList<URI>();
    while (matcher.find()) {
      final String s = matcher.group();
      final String uri = s.substring(1, s.length() - 1);
      try {
        URI createdUri = URI.create(uri);
        result.add(createdUri);
      } catch (IllegalArgumentException e) {
        //failure to create the URI, which means this is bad JSON. Convert
        //to an exception with useful text
        throw new SwiftOperationFailedException(
          String.format(
            "could not convert \"%s\" into a URI." +
            " source: %s " +
            " first JSON: %s",
            uri, path, json.substring(0, 256)));
      }
    }
    return result;
  }

  /**
   * Insert a throttled wait if the throttle delay >0
   * @throws InterruptedIOException if interrupted during sleep
   */
  public void throttle() throws InterruptedIOException {
    int throttleDelay = getThrottleDelay();
    if (throttleDelay > 0) {
      try {
        Thread.sleep(throttleDelay);
      } catch (InterruptedException e) {
        //convert to an IOE
        throw (InterruptedIOException) new InterruptedIOException(e.toString())
          .initCause(e);
      }
    }
  }

  /**
   * Get the current operation statistics
   * @return a snapshot of the statistics
   */
  public List<DurationStats> getOperationStatistics() {
    return swiftRestClient.getOperationStatistics();
  }


  /**
   * Delete the entire tree. This is an internal one with slightly different
   * behavior: if an entry is missing, a {@link FileNotFoundException} is
   * raised. This lets the caller distinguish a file not found with
   * other reasons for failure, so handles race conditions in recursive
   * directory deletes better.
   * <p/>
   * The problem being addressed is: caller A requests a recursive directory
   * of directory /dir ; caller B requests a delete of a file /dir/file,
   * between caller A enumerating the files contents, and requesting a delete
   * of /dir/file. We want to recognise the special case
   * "directed file is no longer there" and not convert that into a failure
   *
   * @param absolutePath  the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if the object was deleted
   * @throws IOException           IO problems
   * @throws FileNotFoundException if a file/dir being deleted is not there -
   *                               this includes entries below the specified path, (if the path is a dir
   *                               and recursive is true)
   */
  public boolean delete(Path absolutePath, boolean recursive) throws IOException {
    Path swiftPath = getCorrectSwiftPath(absolutePath);
    SwiftUtils.debug(LOG, "Deleting path '%s' recursive=%b",
                     absolutePath,
                     recursive);
    boolean askForNewest = true;
    SwiftFileStatus fileStatus = getObjectMetadata(swiftPath, askForNewest);

    //ask for the file/dir status, but don't demand the newest, as we
    //don't mind if the directory has changed
    //list all entries under this directory.
    //this will throw FileNotFoundException if the file isn't there
    FileStatus[] statuses;
    try {
      statuses = listSubPaths(absolutePath, true, askForNewest);
    } catch (IOException e) {
      // absolutePath is nonexistent
      statuses = new FileStatus[0];
    }
    int filecount = statuses.length;
    SwiftUtils.debug(LOG, "Path '%s' %d status entries'",
                     absolutePath,
                     filecount);

    if (filecount == 0) {
      //it's an empty directory or a path
      deleteObject(fileStatus);
      return true;
    }

    if (LOG.isDebugEnabled()) {
      SwiftUtils.debug(LOG, SwiftUtils.fileStatsToString(statuses, "\n"));
    }

    if (filecount == 1 && swiftPath.equals(statuses[0].getPath())) {
      // 1 entry => simple file and it is the target
      //simple file: delete it
      SwiftUtils.debug(LOG, "Deleting simple file %s", absolutePath);
      deleteObject(fileStatus);
      return true;
    }

    //>1 entry implies directory with children. Run through them,
    // but first check for the recursive flag and reject it *unless it looks
    // like a partitioned file (len > 0 && has children)
    if (!fileStatus.isDirectory()) {
      LOG.debug("Multiple child entries but entry has data: assume partitioned");
    } else if (!recursive) {
      //if there are children, unless this is a recursive operation, fail immediately
      throw new SwiftOperationFailedException("Directory " + fileStatus
                                              + " is not empty: "
                                              + SwiftUtils.fileStatsToString(
                                                        statuses, "; "));
    }

    //delete the entries. including ourself.
    for (FileStatus entryStatus : statuses) {
      Path entryPath = entryStatus.getPath();
      try {
        boolean deleted = deleteObject(entryStatus);
        if (!deleted) {
          SwiftUtils.debug(LOG, "Failed to delete entry '%s'; continuing",
                           entryPath);
        }
      } catch (FileNotFoundException e) {
        //the path went away -race conditions.
        //do not fail, as the outcome is still OK.
        SwiftUtils.debug(LOG, "Path '%s' is no longer present; continuing",
                         entryPath);
      }
      throttle();
    }
    //now delete self
    SwiftUtils.debug(LOG, "Deleting base entry %s", absolutePath);
    deleteObject(fileStatus);

    return true;
  }

  /**
   * List all segments in dynamic large object.
   *
   * @param file   SwiftFileStatus of large object
   * @param newest ask for the newest, or can some out of date data work?
   * @return the file statuses, or an empty array if there are no segments
   * @throws IOException           on IO problems
   */
  public FileStatus[] listSegments(FileStatus file, boolean newest)
      throws IOException {
    SwiftObjectPath prefix = ((SwiftFileStatus)file).getDLOPrefix();
    if (prefix == null) {
      return new FileStatus[0];
    }

    final List<FileStatus> objects;
    objects = listDirectory(prefix, true, newest, false);

    final ArrayList<FileStatus> segments;
    segments = new ArrayList<FileStatus>(objects.size());

    for (FileStatus status : objects) {
      if (!status.isDir()) {
        segments.add(status);
      }
    }

    return segments.toArray(new FileStatus[segments.size()]);
  }
}
