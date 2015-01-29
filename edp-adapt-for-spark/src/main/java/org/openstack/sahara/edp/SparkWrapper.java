package org.openstack.sahara.edp;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.Arrays;

public class SparkWrapper {

  public static void main(String[] args) throws Throwable {

    Class<?> configClass
      = Class.forName("org.apache.hadoop.conf.Configuration");
    Method method = configClass.getMethod("addDefaultResource", String.class);
    method.invoke(null, args[0]);

    Class<?> mainClass = Class.forName(args[1]);
    Method mainMethod = mainClass.getMethod("main", String[].class);
    String[] newArgs = Arrays.copyOfRange(args, 2, args.length);
    mainMethod.invoke(null, (Object) newArgs);
  }
}
