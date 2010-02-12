require 'java'
require 'backport-util-concurrent-2.2.jar'
require 'log4j-1.2.9.jar'
require 'slf4j-api-1.4.0.jar'
require 'slf4j-log4j12-1.4.0.jar'
require 'mina-core-0.9.5-SNAPSHOT.jar'
require 'openamq-jms-1.2c1.jar'
require 'jruby/rack/queues/local'

class JRuby::Rack::Queues::OpenAMQ
  
  def self.configure
    openamq = new
    yield openamq
  ensure
    scheme, userinfo, host, port, _unused, vhost = URI.split(openamq.url)
    user, pass = userinfo.split ':'
    raise "bad scheme #{scheme}" unless scheme == 'amqp'

    openamq.user = user
    openamq.pass = pass
    openamq.host = host
    openamq.port = port
    openamq.vhost = vhost

    openamq.set_init_parameters
    qm = Java::OrgJrubyRackJms::OpenAMQQueueManager.new
    ::JRuby::Rack::Queues::Registry.start_queue_manager(qm)
    at_exit do
      ::JRuby::Rack::Queues::Registry.stop_queue_manager
    end
  end

  attr_writer :url, :topics, :queues
  attr_accessor :user, :pass, :host, :port, :vhost

  def url
    @url ||= "amqp://guest:guest@localhost:5672/"
  end

  def queues
    @queues ||= []
  end

  def topics
    @topics ||= []
  end

  def set_init_parameters
    p = ::JRuby::Rack::Queues::LocalContext.init_parameters
    p['openamq.user'] = user
    p['openamq.pass'] = pass
    p['openamq.host'] = host
    p['openamq.port'] = port
    p['openamq.vhost'] = vhost
  end
end
