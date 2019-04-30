# frozen_string_literal: true

module Restforce
  module Concerns
    module Streaming
      # Public: Subscribe to a PushTopic
      #
      # channels - The name of the PushTopic channel(s) to subscribe to.
      # block    - A block to run when a new message is received.
      #
      # Returns a Faye::Subscription
      def subscribe(channels, options = {}, &block)
        Array(channels).each { |channel|
          replay_handlers[channel] = options.fetch(:replay, -2)
        }
        channel_topics = Array(channels).map { |channel| "/topic/#{channel}" }
        faye.subscribe(channel_topics, &block)
      end

      # Registers a block which will be called once we recieve a Disconnect message from SalesForce
      # This block can be used to restart the subscription
      def on_disconnect(&block)
        @disconnect_block = block
      end

      # Public: Faye client to use for subscribing to PushTopics
      def faye
        unless options[:instance_url]
          raise 'Instance URL missing. Call .authenticate! first.'
        end

        url = "#{options[:instance_url]}/cometd/#{options[:api_version]}"

        @faye ||= Faye::Client.new(url).tap do |client|
          client.set_header 'Authorization', "OAuth #{options[:oauth_token]}"

          client.bind 'transport:down' do
            Restforce.log "[COMETD DOWN]"
            client.set_header 'Authorization', "OAuth #{authenticate!.access_token}"
          end

          client.bind 'transport:up' do
            Restforce.log "[COMETD UP]"
          end

          client.add_extension ReplayExtension.new(replay_handlers)
          client.add_extension OnDisconnectExtension.new(@disconnect_block) if @disconnect_block
        end
      end

      def replay_handlers
        @_replay_handlers ||= {}
      end

      class OnDisconnectExtension
        def initialize(disconnect_block)
          @disconnect_block = disconnect_block
        end

        def incoming(message, callback)
          ap message
          Restforce.log ("OnDisconnectExtension - recieved a message #{message}")
          if message['channel'] == "/meta/disconnect"
            Restforce.log ("OnDisconnectExtension - recieved a disconenct message #{message}")
            @disconnect_block.call
          end
          callback.call(message)
        end

        def outgoing(message, callback)
          callback.call(message)
        end
      end

      class ReplayExtension
        def initialize(replay_handlers)
          @replay_handlers = replay_handlers
        end

        def incoming(message, callback)
          callback.call(message).tap do
            channel = message.fetch('channel').gsub('/topic/', '')
            replay_id = message.fetch('data', {}).fetch('event', {})['replayId']

            handler = @replay_handlers[channel]
            if !replay_id.nil? && !handler.nil? && handler.respond_to?(:[]=)
              # remember the last replay_id for this channel
              handler[channel] = replay_id
            end
          end
        end

        def outgoing(message, callback)
          # Leave non-subscribe messages alone
          unless message['channel'] == '/meta/subscribe'
            return callback.call(message)
          end

          channel = message['subscription'].gsub('/topic/', '')

          # Set the replay value for the channel
          message['ext'] ||= {}
          message['ext']['replay'] = {
            "/topic/#{channel}" => replay_id(channel)
          }

          # Carry on and send the message to the server
          callback.call(message)
        end

        private

        def replay_id(channel)
          handler = @replay_handlers[channel]
          if handler.is_a?(Integer)
            handler # treat it as a scalar
          elsif handler.respond_to?(:[])
            # Ask for the latest replayId for this channel
            handler[channel]
          else
            # Just pass it along
            handler
          end
        end
      end
    end
  end
end
