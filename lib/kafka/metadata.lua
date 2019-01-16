local encode = require "kafka.encode"
local decode = require "kafka.decode"

local concat = table.concat

local leaders, empty_table = {}, {}
local pkt = {
	[1] = nil,                      -- size
	[2] = string.char(0x00, 0x03),  -- api_key
	[3] = string.char(0x00, 0x07),  -- api_version
	[4] = string.char(0,0,0,0),     -- correlation_id
	[5] = nil,                      -- client_id
	[6] = string.char(0,0,0,1),     -- ntopics
	[7] = nil,                      -- topic_1
	[8] = string.char(0x01)         -- allow_auto_topic_creation
}

local response = decode.response{
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

return function(client, topic, partition, refresh)
	-- todo: dont return nil (must return table)

	if not client then
		return nil, "client must be passed"
	elseif not client.client_id then
		return nil, "client_id missing"
	elseif not topic then
		return nil, "topic missing"
	elseif not partition then
		return nil, "partition missing"
	end

	if not refresh and leaders[topic] and leaders[topic][partition] then
		return leaders[topic][partition]
	end

	pkt[1] = encode.int32(8 + 2 + #client.client_id + 4 + 2 + #topic + 1)
	pkt[5] = encode.string(client.client_id)
	pkt[7] = encode.string(topic)

	local data, err = client:send(concat(pkt))
	if not data then
		return nil, err
	end

	data, err = response(data)
	if not data then
		return nil, err
	end

	leaders[topic] = leaders[topic] or {}

	local brokers, _topic = {}, data.topic_metadata[1]
	for _,broker in ipairs(data.brokers) do
		brokers[broker.node_id] = broker
	end

	for _,partition in ipairs(_topic.partition_metadata) do
		leaders[topic][partition.partition] = {brokers[partition.leader]}
	end

	return leaders[topic][partition] or empty_table
end
