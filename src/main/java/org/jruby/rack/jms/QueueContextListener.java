/*
 * Copyright 2007-2009 Sun Microsystems, Inc.
 * This source code is available under the MIT license.
 * See the file LICENSE.txt for details.
 */

package org.jruby.rack.jms;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.jruby.rack.servlet.ServletRackContext;

/**
 *
 * @author nicksieger
 */
public class QueueContextListener implements ServletContextListener {
    private QueueManagerFactory factory;
    private QueueManager queueManager;
    
    public QueueContextListener() {
        this.factory = null;
    }
    
    public QueueContextListener(QueueManagerFactory qmf) {
        this.factory = qmf;
    }
    
    public void contextInitialized(ServletContextEvent event) {
	queueManager = (QueueManager) event.getServletContext().getAttribute(QueueManager.MGR_KEY);
    }

    public void contextDestroyed(ServletContextEvent event) {
        if (queueManager != null) {
            queueManager.destroy();
        }
    }

    private QueueManagerFactory newQueueManagerFactory() {
        if (factory != null) {
            return factory;
        }
        return new QueueManagerFactory() {
            public QueueManager newQueueManager() {
                return new DefaultQueueManager();
            }
        };
    }
}
