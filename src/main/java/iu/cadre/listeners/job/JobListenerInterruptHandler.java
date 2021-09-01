package iu.cadre.listeners.job;

import iu.cadre.listeners.job.ListenerStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.lang.Runnable;


public class JobListenerInterruptHandler implements Runnable {
   private Logger LOG;
   private ListenerStatus listenerStatus;

   public JobListenerInterruptHandler(ListenerStatus listenerStatus) {
      LOG = LoggerFactory.getLogger(JobListenerInterruptHandler.class);
      this.listenerStatus = listenerStatus;
   }

   public void run() {
      try {
         listenerStatus.updateAsStopped();
      } catch (Exception e) {
        LOG.error("Interrupt handler failed to send final listener status update: " + e.getMessage());
      }
   }
}
