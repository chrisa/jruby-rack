package org.jruby.rack.jms;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.ArrayList;
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
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jruby.Ruby;
import org.jruby.RubyObjectAdapter;
import org.jruby.RubyRuntimeAdapter;
import org.jruby.javasupport.JavaEmbedUtils;
import org.jruby.rack.RackApplication;
import org.jruby.rack.RackApplicationFactory;
import org.jruby.rack.RackContext;
import org.jruby.runtime.builtin.IRubyObject;

import org.openamq.client.AMQQueue;
import org.openamq.client.AMQTopic;
import org.openamq.client.AMQConnectionFactory;
import org.openamq.client.message.JMSBytesMessage;

public class OpenAMQQueueManager implements QueueManager {
    private RubyRuntimeAdapter rubyRuntimeAdapter = JavaEmbedUtils.newRuntimeAdapter();
    private RubyObjectAdapter rubyObjectAdapter = JavaEmbedUtils.newObjectAdapter();
    private ConnectionFactory connectionFactory = null;
    private Connection conn = null;
    private RackContext context;
    private Context jndiContext;
    private Map<String, ArrayList<Session>> queue_sessions = 
	new HashMap<String, ArrayList<Session>>();

    public OpenAMQQueueManager() {
    }

    public void init(RackContext context) throws Exception {
        this.context = context;

	if (connectionFactory == null) {
	    String host = context.getInitParameter("openamq.host");
	    String port = context.getInitParameter("openamq.port");
	    String user = context.getInitParameter("openamq.user");
	    String pass = context.getInitParameter("openamq.pass");
	    String vhost = context.getInitParameter("openamq.vhost");
	    int port_number = (int) Integer.parseInt(port);
            connectionFactory = (ConnectionFactory) new AMQConnectionFactory(host, 
									     port_number, 
									     user, 
									     pass, 
									     vhost);
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

	ArrayList<Session> sessions = (ArrayList<Session>) queue_sessions.get(queueName);
	if (sessions == null) {
	    sessions = new ArrayList<Session>();
	}

	try {
	    Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	    AMQQueue dest = new AMQQueue(queueName);
	    MessageConsumer consumer = session.createConsumer(dest);
	    consumer.setMessageListener(new RubyObjectMessageListener(queueName));
	    conn.start();
	    sessions.add(session);
	    queue_sessions.put(queueName, sessions);
	} catch (JMSException e) {
	    context.log("Unable to listen to '"+queueName+"': JMS: " + e.getMessage(), e);
	}
    }

    public synchronized void close(String queueName) {
	ArrayList<Session> sessions = (ArrayList<Session>) queue_sessions.remove(queueName);
	if (sessions != null) {
	    try {
		for (Iterator it = sessions.iterator(); it.hasNext();) {
		    Session session = (Session)it.next();
		    session.close();
		}
	    } catch (JMSException e) {
		context.log("Unable to close session for queue " + queueName + ": " + e.getMessage());
	    }
	    if (queue_sessions.isEmpty()) {
		closeConnection();
	    }
	}
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }
    
    public Object lookup(String name) throws javax.naming.NamingException {
        return null;
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

    private class RubyObjectMessageListener implements MessageListener {
        private String queueName;
        private RackApplicationFactory rackFactory;
        public RubyObjectMessageListener(String name) {
            this.queueName = name;
            this.rackFactory = context.getRackFactory();
        }

        public void onMessage(Message message) {
	    JMSBytesMessage bytesMessage = (JMSBytesMessage) message;
            RackApplication app = null;
            try {
                app = rackFactory.getApplication();
		Ruby runtime = app.getRuntime();
		IRubyObject obj = rubyRuntimeAdapter.eval(runtime, "JRuby::Rack::Queues::Registry");
		rubyObjectAdapter.callMethod(obj, "receive_message", new IRubyObject[] {
			JavaEmbedUtils.javaToRuby(runtime, queueName),
			JavaEmbedUtils.javaToRuby(runtime, bytesMessage)});
            } catch (Exception e) {
                context.log("exception during message reception: " + e.getMessage(), e);
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		String stacktrace = sw.toString();
		context.log(stacktrace);
            } finally {
                if (app != null) {
                    rackFactory.finishedWithApplication(app);
                }
            }
        }
    }
}
