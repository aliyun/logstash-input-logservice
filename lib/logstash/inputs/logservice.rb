require 'java'
require "logstash/namespace"
require "logstash/event"
require "logstash/environment"
require "logstash/inputs/base"

root_dir = File.expand_path(File.join(File.dirname(__FILE__), ".."))
LogStash::Environment.load_runtime_jars! File.join(root_dir, "vendor")

java_import 'com.aliyun.log.logstash.LogHubStarter'
java_import 'com.aliyun.log.logstash.LogstashLogHubProcessor'

class LogStash::Inputs::LogService < LogStash::Inputs::Base
  attr_accessor :worker
  def initialize(*args)
    super(*args)
  end

  class LogHubProcessor < LogstashLogHubProcessor
    attr_accessor :queue
    def showContent(logmap)
        event = LogStash::Event.new(logmap)
        # this will block if output_queue is full. output_queue size is 20
        @queue << event
    end
  end

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
    @logger.info("Running logstash-input-logservice",:local_address => @local_address)
    @handler = LogHubProcessor.new()
    @handler.setCheckpointSecond(@checkpoint_second)
    @handler.setIncludeMeta(@include_meta)
    @handler.queue = queue
    LogHubStarter.startWorker(@handler, @endpoint, @access_id, @access_key, @project, @logstore, @consumer_group, @consumer_name + @ip_suffix + @process_pid, @position)
    rescue Exception => e
        @logger.error("Start logstash-input-logservice", :endpoint => @endpoint, :project => @project, :logstore => @logstore,
            :consumer_group => @consumer_group, :consumer_name => @consumer_name, :position => @position,
            :checkpoint_second => @checkpoint_second, :include_meta => @include_meta, :consumer_name_with_ip => @consumer_name_with_ip, :exception => e)
  end

  def teardown
    @interrupted = true
    finished
  end # def teardown
end 

