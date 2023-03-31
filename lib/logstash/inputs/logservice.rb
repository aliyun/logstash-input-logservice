require 'java'
require "logstash/namespace"
require "logstash/event"
require "logstash/environment"
require "logstash/inputs/base"
require 'json'
require "stud/interval"

root_dir = File.expand_path(File.join(File.dirname(__FILE__), ".."))
LogStash::Environment.load_runtime_jars! File.join(root_dir, "vendor")

java_import 'com.aliyun.log.logstash.LogHubStarter'
java_import 'com.aliyun.log.logstash.LogstashLogHubProcessor'

class LogStash::Inputs::LogService < LogStash::Inputs::Base
  # attr_accessor :worker
  def initialize(*args)
    super(*args)
  end

  default :codec, "json"

  config_name "logservice"

  config :endpoint, :validate => :string, :required => true
  config :access_id, :validate => :string, :required => true
  config :access_key, :validate => :string, :required => true
  config :project, :validate => :string, :required => true
  config :logstore, :validate => :string, :required => true
  config :consumer_group, :validate => :string, :required => true
  config :consumer_name, :validate => :string, :required => true
  config :position, :validate => :string, :required => true
  config :checkpoint_second, :validate => :number, :default => 30
  config :include_meta, :validate => :boolean, :default => true
  config :consumer_name_with_ip, :validate => :boolean, :default => true

  config :proxy_host, :validate => :string, :default => nil
  config :proxy_port, :validate => :number, :default => 0
  config :proxy_username, :validate => :string, :default => nil
  config :proxy_password, :validate => :string, :default => nil
  config :proxy_domain, :validate => :string, :default => nil
  config :proxy_workstation, :validate => :string, :default => nil

  config :fetch_interval_millis, :validate => :number, :default => 200

  # Processor = com.aliyun.log.logstash
  public
  def register
    @logger.info("Init logstash-input-logservice", :endpoint => @endpoint, :project => @project, :logstore => @logstore,
        :consumer_group => @consumer_group, :consumer_name => @consumer_name, :position => @position,
        :checkpoint_second => @checkpoint_second, :include_meta => @include_meta ,:consumer_name_with_ip => @consumer_name_with_ip)
  end

  def run(queue)
    @local_address = java.net.InetAddress.getLocalHost().getHostAddress();
    @ip_suffix = ''
    if @consumer_name_with_ip
        @ip_suffix = '_' + @local_address
    end
    @process_pid = "_#{Process.pid}"
    @logger.info("Running logstash-input-logservice", :endpoint => @endpoint, :project => @project,
    :logstore => @logstore, :consumer_group => @consumer_group, :consumer_name => @consumer_name,
    :position => @position, :checkpoint_second => @checkpoint_second, :include_meta => @include_meta,
    :consumer_name_with_ip => @consumer_name_with_ip, :local_address => @local_address)
    @blockingQueue = java.util.concurrent.LinkedBlockingQueue.new(1000)
    @logHubStarter = LogHubStarter.new()
    @logHubStarter.startWorker(@endpoint, @access_id, @access_key, @project, @logstore, @consumer_group, @consumer_name + @ip_suffix + @process_pid, @position, @checkpoint_second, @include_meta, @blockingQueue, @proxy_host, @proxy_port, @proxy_username, @proxy_password, @proxy_domain, @proxy_workstation, @fetch_interval_millis)

    consume(queue)

    rescue Exception => e
        @logger.error("Start logstash-input-logservice", :endpoint => @endpoint, :project => @project, :logstore => @logstore,
            :consumer_group => @consumer_group, :consumer_name => @consumer_name, :position => @position,
            :checkpoint_second => @checkpoint_second, :include_meta => @include_meta, :consumer_name_with_ip => @consumer_name_with_ip, :exception => e)

  end

  def consume(queue)
         while !stop?
             while !@blockingQueue.isEmpty
                 begin
                     msg = @blockingQueue.poll
                     event = LogStash::Event.new(msg)
                     decorate(event)
                     queue << event
                 rescue Exception => e
                     @logger.error("Consume logstash-input-logservice", :endpoint => @endpoint, :project => @project, :logstore => @logstore,
                                 :consumer_group => @consumer_group, :consumer_name => @consumer_name, :position => @position,
                                 :checkpoint_second => @checkpoint_second, :include_meta => @include_meta, :consumer_name_with_ip => @consumer_name_with_ip, :exception => e)
                     retry
                 end
             end
             sleep(0.01)
        end # loop

  end
  def stop
    @logHubStarter.stopWorker()
  end

  def teardown
    @interrupted = true
    finished
  end # def teardown
end 

