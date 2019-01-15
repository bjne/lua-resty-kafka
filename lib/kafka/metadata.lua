local encode = require "kafka.encode"
local response = require "kafka.response".new{
	"int32:throttle_time_ms",
	"array:brokers", {
		"int32:node_id",
		"string:host",
		"int32:port",
		"string:rack"
	},
	"string:cluster_id",
	"int32:controller_id",
	"array:topic_metadata", {
		"int16:error_code",
		"string:topic",
		"boolean:is_internal",
		"array:partition_metadata", {
			"int16:error_code",
			"int32:partition",
			"int32:leader",
			"int32:leader_epoch",
			"array:replicas", {
				"int32:node"
			},
			"array:isr", {
				"int32:node"
			},
			"array:offline_replicas", {
				"int32:node"
			}
		}
	}
}

local empty_table, pkt = {}, {
	[1] = nil,                     -- size
	[2] = string.char(0x00, 0x03), -- api_key
	[3] = string.char(0x00, 0x07), -- api_version
	[4] = string.char(0,0,0,0),    -- correlation_id
	[5] = nil,                     -- client_id
	[6] = string.char(0,0,0,1),    -- ntopics
	[7] = nil,                     -- topic_1
	[8] = string.char(0x01)        -- allow_auto_topic_creation
}

local _M = {}
local mt = {
	__index = {
		refresh = function(self, partition)
			local data, err = self.client:send(self.pkt)
			if not data then
				return nil, err
			end

			data, err = response(data)
			if not data then
				return nil, err
			end

			local brokers, leaders, topic = {}, {}, data.topic_metadata[1]
			for _,broker in ipairs(data.brokers) do
				brokers[broker.node_id] = broker
			end

			for _,partition in ipairs(topic.partition_metadata) do
				leaders[partition.partition] = {brokers[partition.leader]}
			end

			self.leaders = leaders

			return partition and (leaders[partition] or empty_table)
		end
	},
	__call = function(self, part, refresh)
		return (refresh or not self.leaders[part]) and self:refresh(part)
			or (self.leaders[part] or empty_table)
	end
}


_M.new = function(config, client)
	if not client then
		return nil, "client must be passed"
	elseif not config.client_id then
		return nil, "client_id missing"
	elseif not config.topic then
		return nil, "topic missing"
	end

	pkt[1] = encode.int32(17 + #config.client_id + #config.topic)
	pkt[5] = encode.string(config.client_id)
	pkt[7] = encode.string(config.topic)

	return setmetatable({
		client = client,
		leaders = {},
		pkt = table.concat(pkt)
	}, mt)
end

return _M
