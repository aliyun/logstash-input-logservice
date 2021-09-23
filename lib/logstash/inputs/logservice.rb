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
  attr_accessor :worker
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
  config :queue_size, :validate => :number, :default => 1000
  Processor = com.aliyun.log.logstash
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
    LogHubStarter.startWorker(@endpoint, @access_id, @access_key, @project, @logstore, @consumer_group, @consumer_name + @ip_suffix + @process_pid, @position, @checkpoint_second, @include_meta, @queue_size)
   
    consume(queue)

    rescue Exception => e
        @logger.error("Start logstash-input-logservice", :endpoint => @endpoint, :project => @project, :logstore => @logstore,
            :consumer_group => @consumer_group, :consumer_name => @consumer_name, :position => @position,
            :checkpoint_second => @checkpoint_second, :include_meta => @include_meta, :consumer_name_with_ip => @consumer_name_with_ip, :exception => e)

  end
  
  def consume(queue)
         while !stop?
             while !Processor.LogstashLogHubProcessor.queueCache.isEmpty
                 begin
                     textmap = Processor.LogstashLogHubProcessor.queueCache.poll
                     event = LogStash::Event.new(textmap)
                     decorate(event)
                     queue << event
                 rescue Exception => e
                     @logger.error("Consume logstash-input-logservice", :endpoint => @endpoint, :project => @project, :logstore => @logstore,
                                 :consumer_group => @consumer_group, :consumer_name => @consumer_name, :position => @position,
                                 :checkpoint_second => @checkpoint_second, :include_meta => @include_meta, :consumer_name_with_ip => @consumer_name_with_ip, :exception => e)
                     retry
                 end
             end
          Stud.stoppable_sleep(@checkpoint_second) { stop? }
        end # loop

  end  

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end

  def teardown
    @interrupted = true
    finished
  end # def teardown
end 

