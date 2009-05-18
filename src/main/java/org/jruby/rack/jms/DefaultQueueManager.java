/*
 * Copyright 2007-2009 Sun Microsystems, Inc.
 * This source code is available under the MIT license.
 * See the file LICENSE.txt for details.
 */

package org.jruby.rack.jms;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.jruby.Ruby;
import org.jruby.RubyObjectAdapter;
import org.jruby.RubyRuntimeAdapter;
import org.jruby.javasupport.JavaEmbedUtils;
import org.jruby.rack.RackApplication;
import org.jruby.rack.RackApplicationFactory;
import org.jruby.rack.RackContext;
import org.jruby.runtime.builtin.IRubyObject;

/**
 *
 * @author nicksieger
 */
public class DefaultQueueManager implements QueueManager {
    private ConnectionFactory connectionFactory = null;
    private Connection conn = null;
    private HashMap<String, Session> sessions = new HashMap<String, Session>();
    private RackContext context;
    private Context jndiContext;
    private RubyRuntimeAdapter rubyRuntimeAdapter = JavaEmbedUtils.newRuntimeAdapter();
    private RubyObjectAdapter rubyObjectAdapter = JavaEmbedUtils.newObjectAdapter();

    public DefaultQueueManager() {
    }

    public DefaultQueueManager(ConnectionFactory qcf, Context ctx) {
        this.connectionFactory = qcf;
        this.jndiContext = ctx;
    }
    
    public void init(RackContext context) throws Exception {
        this.context = context;
        String jndiName = context.getInitParameter("jms.connection.factory");
        if (jndiName != null && connectionFactory == null) {
            Properties properties = new Properties();
            String jndiProperties = context.getInitParameter("jms.jndi.properties");
            if (jndiProperties != null) {
                properties.load(new ByteArrayInputStream(jndiProperties.getBytes("UTF-8")));
            }
            jndiContext = new InitialContext(properties);
            connectionFactory = (ConnectionFactory) jndiContext.lookup(jndiName);
        }
    }

    public synchronized void listen(String queueName) {
	if (conn == null) {
	    try {
		conn = connectionFactory.createConnection();
	    } catch (JMSException e) {
		context.log("Unable to connect to broker: " + e.getMessage(), e);
		return;
	    }
	}
	
	try {
	    Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	    Destination dest = (Destination) lookup(queueName);
	    MessageConsumer consumer = session.createConsumer(dest);
	    consumer.setMessageListener(new RubyObjectMessageListener(queueName));
	    conn.setExceptionListener(new ConnectionExceptionListener());
	    conn.start();
	    sessions.put(queueName, session);
	} catch (Exception e) {
	    context.log("Unable to listen to '"+queueName+"': " + e.getMessage(), e);
	}
    }

    public synchronized void close(String queueName) {
	Session session = (Session) sessions.remove(queueName);
	try {
	    session.close();
	} catch (JMSException e) {
	    context.log("Unable to close session for queue " + queueName + ": " + e.getMessage());
	}
	if (sessions.isEmpty()) {
	    closeConnection();
	}
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }
    
    public Object lookup(String name) throws javax.naming.NamingException {
        return jndiContext.lookup(name);
    }

    public void destroy() {
	closeConnection();
        connectionFactory = null;
    }

    private void closeConnection() {
	if (conn != null) {
	    try {
		conn.close();
		conn = null;
	    } catch (JMSException e) {
		context.log("exception while closing connection: " + e.getMessage(), e);
	    }
	}
    }

    private class ConnectionExceptionListener implements ExceptionListener {

	public ConnectionExceptionListener() {
	}

	public void onException (JMSException e) {
	    context.log("JMSException: " + e.getMessage());
	}
    }

    private class RubyObjectMessageListener implements MessageListener {
        private String queueName;
        private RackApplicationFactory rackFactory;
        public RubyObjectMessageListener(String name) {
            this.queueName = name;
            this.rackFactory = context.getRackFactory();
        }

        public void onMessage(Message message) {
            RackApplication app = null;
            try {
                app = rackFactory.getApplication();
		Ruby runtime = app.getRuntime();
		IRubyObject obj = rubyRuntimeAdapter.eval(runtime, "JRuby::Rack::Queues::Registry");
		rubyObjectAdapter.callMethod(obj, "receive_message", new IRubyObject[] {
			JavaEmbedUtils.javaToRuby(runtime, queueName),
			JavaEmbedUtils.javaToRuby(runtime, message)});
            } catch (Exception e) {
                context.log("exception during message reception: " + e.getMessage(), e);
            } finally {
                if (app != null) {
                    rackFactory.finishedWithApplication(app);
                }
            }
        }
    }
}
