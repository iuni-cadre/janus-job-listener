package iu.cadre.listeners.job;

import java.lang.Exception;

public class ClusterNotInitializedException extends Exception {
   ClusterNotInitializedException(String s) {
      super(s);
   }

   ClusterNotInitializedException(String s, Throwable e) {
      super(s,e);
   }
}

