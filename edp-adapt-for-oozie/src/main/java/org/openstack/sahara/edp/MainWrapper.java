package org.openstack.sahara.edp;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.Arrays;

public class MainWrapper {

  public static void main(String[] args) throws Throwable {

    // Load oozie configuration file
    String actionConf = System.getProperty("oozie.action.conf.xml");
    if (actionConf != null) {
      Class<?> configClass
        = Class.forName("org.apache.hadoop.conf.Configuration");
      Method method = configClass.getMethod("addDefaultResource", String.class);
      method.invoke(null, "action.xml");
    }

    SecurityManager originalSecurityManager = System.getSecurityManager();
    WrapperSecurityManager newSecurityManager
      = new WrapperSecurityManager(originalSecurityManager);
    System.setSecurityManager(newSecurityManager);

    Class<?> mainClass = Class.forName(args[0]);
    Method mainMethod = mainClass.getMethod("main", String[].class);
    String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
    Throwable exception = null;
    try {
      mainMethod.invoke(null, (Object) newArgs);
    } catch (InvocationTargetException e) {
      if (!newSecurityManager.getExitInvoked()) {
        exception = e.getTargetException();
      }
    }

    System.setSecurityManager(originalSecurityManager);

    if (exception != null) {
      throw exception;
    }
    if (newSecurityManager.getExitInvoked()) {
      System.exit(newSecurityManager.getExitCode());
    }
  }

  static class WrapperSecurityManager extends SecurityManager {
    private static boolean exitInvoked = false;
    private static int firstExitCode;
    private SecurityManager securityManager;

    public WrapperSecurityManager(SecurityManager securityManager) {
      this.securityManager = securityManager;
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      if (securityManager != null) {
        // check everything with the original SecurityManager
        securityManager.checkPermission(perm, context);
      }
    }

    @Override
    public void checkPermission(Permission perm) {
      if (securityManager != null) {
        // check everything with the original SecurityManager
        securityManager.checkPermission(perm);
      }
    }

    @Override
    public void checkExit(int status) throws SecurityException {
      if (!exitInvoked) {
        // save first System.exit status code
        exitInvoked = true;
        firstExitCode = status;
      }
      throw new SecurityException("Intercepted System.exit(" + status + ")");
    }

    public static boolean getExitInvoked() {
      return exitInvoked;
    }

    public static int getExitCode() {
      return firstExitCode;
    }
  }
}
