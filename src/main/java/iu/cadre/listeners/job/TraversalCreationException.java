package iu.cadre.listeners.job;

import java.lang.Exception;

public class TraversalCreationException extends Exception {
   TraversalCreationException(String s) {
      super(s);
   }

   TraversalCreationException(String s, Throwable e) {
      super(s,e);
   }
}

