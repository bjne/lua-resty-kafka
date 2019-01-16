local client = require "kafka.client"
local crc32c = require "kafka.crc32c"
local encode = require "kafka.encode"
local new_tab = require "table.new"
local zstandard = require "kafka.zstd"
local response = require "kafka.response".new{
	"array:response", { 
		"string:topic",
		"array:partition", {
			"int32:id",
			"int16:error_code",
			"int64:base_offset",
			"int64:log_append_time",
			"int64:log_start_offset"
		},
		"int32:throttle_time_ms"
	}
}

local concat = table.concat
local char = string.char
local byte = string.byte
local sub = string.sub

local _M = { _VERSION = 0.1 }
local mt = { __index = _M }

local zstd -- must be initialized per worker (on add)
local record = { [2] = char(0x00), [5] = char(0x01) } -- attributes, key_len
local stat, worker_count = ngx.shared.kafka_stats, ngx.worker.count()
local stat_incr, stat_rate = function()end, function()end



local function get_free_record_batch(self)
	local cur_record_batch = self.record_batch

	if not cur_record_batch.lock then
		return cur_record_batch
	end

	local old_record_batch, new_record_batch = cur_record_batch

	while new_record_batch ~= cur_record_batch do
		new_record_batch = (new_record_batch or cur_record_batch).next

		if not new_record_batch.lock then
			self.record_batch = new_record_batch

			return new_record_batch
		end

		if new_record_batch.max_timestamp < old_record_batch.max_timestamp then
			old_record_batch = new_record_batch
		end
	end

	if old_record_batch.max_timestamp + self.max_timeout > ngx.now() then
		return nil, stat_incr("record_batch not available")
	end

	old_record_batch.length, old_record_batch.size, old_record_batch.lock = 0, 0
	self.record_batch = old_record_batch

	return old_record_batch, stat_incr("record_batch timed out")
end

if stat then
	local k = {}

	local _key = function(key, div)
		k[1],k[2],k[3] = k[1] or char(ngx.worker.id()), char(div or 1), key
		return concat(k)
	end

	stat_rate = function(key, val)
		if key then
			key = _key(key, worker_count)
			stat:set(key, tonumber((stat:get(key)) or val)/10*(10-1) + val/10)
		end
	end

	stat_incr = function(key, val)
		if key and val ~= 0 then
			stat:incr(_key(key), val or 1, 0)
		end
	end
end

function _M.stats(worker)
	if not stat then
		return nil, "shared dictionary kafka_stats missing in configuration"
	end

	worker = worker and tonumber(worker)

	local res, val, wrk, div = {}
	for _, key in ipairs(stat:get_keys()) do
		wrk, div = byte(key, 1, 2)
		if wrk == (worker or wrk) then
			key, val = sub(key, 3), stat:get(key)
			res[key] = (res[key] or 0) + (val / (worker and 1 or div))
		end
	end

	for k,v in pairs(res) do
		ngx.say(k, "=", v)
	end
end

function _M.new(config)
	if not config.topic then
		return nil, "no topic supplied"
	end

	config.client_id = config.client_id or 'ngx'

	local client, err = client.new(config)
	if not client then
		return nil, err
	end


	local self = {
		client                 = client,

		topic                  = config.topic,
		client_id              = config.client_id,
		partition							 = config.partition,

		batch_num              = config.batch_num or 4,
		batch_size             = config.batch_size or 1024 * 1024,
		batch_length           = config.batch_length or 1000,

		required_acks          = config.required_acks or 1,
		timeout                = config.timeout or 1500,
		max_timeout            = config.max_timeout or 2000,
		zstd_compression_level = config.zstd_compression_level or 9,
		flush_interval				 = config.flush_interval or 2,

		offset = 8,
		error_code_offset      = 4 + 4 + 2 + #config.topic + 4 + 4
	}

	if type(config.partition or 0) == "number" then
		self.partition = function() return config.partition or 0 end
	end
	ngx.log(ngx.ERR, "partition: ", self.partition())

	local request_header, produce_header = {
		char(0x00, 0x00),                    -- api_key
		char(0x00, 0x07),                    -- api_version
		char(0,0,0,0),                       -- correlation_id
		encode.string(self.client_id)        -- client_id
	}, {
		encode.string(""),                   -- transaction_id
		encode.int16(self.required_acks),    -- required_acks
		encode.int32(self.timeout),          -- timeout
		encode.int32(1),                     -- number of topics
	}

	-- (request_header) + (produce_header)
	self.header_length = (10 + #self.client_id) + (26 + #self.topic)

	local record_batch_1, record_batch
	for i=1, self.batch_num do
		local r = new_tab(self.offset + self.batch_length + 1 + 1, 5)

		if not record_batch then
			record_batch, record_batch_1, self.record_batch = r, r, r
		else
			record_batch.next = r
			record_batch = record_batch.next

			record_batch.next = i == self.batch_num and record_batch_1
		end

		record_batch[1] = encode.int16(4)    -- attributes (zstd)
		record_batch[5] = encode.int64(-1)   -- producer_id
		record_batch[6] = encode.int16(-1)   -- producer_epoch
		record_batch[7] = encode.int32(-1)   -- first_sequence (not used)

		record_batch.size, record_batch.length, record_batch.pkt = 0, 0, {
			[01] = nil,                       -- size (int32),
			[02] = concat(request_header),
			[03] = concat(produce_header),
			[04] = encode.string(self.topic), -- config.topic
			[05] = encode.int32(1),           -- num_partitions
			[06] = nil,                       -- partition_id (int32)
			[07] = nil,                       -- message_set_size (int32)

			[08] = encode.int64(0),           -- offset
			[09] = nil,                       -- size in bytes (int32)
			[10] = encode.int32(-1),          -- partition leader epoch
			[11] = char(0x02),                -- magic
			[12] = nil,                       -- crc32c (int32)

			[13] = nil                        -- data
		}
	end


	local hdl, err = ngx.timer.every(self.flush_interval,
		function(premature, self)
			local first_timestamp = self.record_batch.first_timestamp or math.huge
			return first_timestamp + self.flush_interval <= ngx.now() and self:send()
		end,
	self)

	return setmetatable(self, mt)
end

function _M:send()
	local record_batch, idx = self.record_batch

	if record_batch.lock or record_batch.length == 0 then
		return
	end

	record_batch.lock, idx = true, record_batch.length + self.offset
	record_batch[idx + 1], record_batch[idx + 2] = zstd:finalize()

	record_batch[2] = encode.int32(record_batch.length - 1)
	record_batch[3] = encode.int64(record_batch.first_timestamp)
	record_batch[4] = encode.int64(record_batch.max_timestamp)
	record_batch[8] = encode.int32(record_batch.length)

	local pkt, data = record_batch.pkt, concat(record_batch, "", 1, record_batch.length + self.offset + 1)

	--local pkt, data = record_batch.pkt, concat(record_batch)
	local size = #data + 9                    -- record batch header

	stat_rate('compression ratio', record_batch.size / (size - 49))
	stat_rate('average records', record_batch.length)


	--local partition = self.partition(record_batch)

	pkt[01] = encode.int32(size + 12 + self.header_length)
	pkt[06] = self.partition(record_batch)    -- partition_id
	pkt[07] = encode.int32(size + 12)         -- messageset size
	pkt[09] = encode.int32(size)              -- record batch length
	pkt[12] = encode.int32(crc32c(data))
	pkt[13] = data

	ngx.timer.at(0, function(premature, record_batch)
		local partition, data, err = record_batch.pkt[06]
		record_batch.pkt[06] = encode.int32(partition)

		for i=1,2 do

			data, err = self.client:send(record_batch.pkt, partition, i==2)

			if not data then
				stat_incr(err)
				break
			end

			err = byte(data, self.error_code_offset + 2)

			if not (i == 1 and err == 6) then
				stat_incr('error_code_' .. tostring(err), err == 0 and 0)
				break
			end
		end

		stat_incr('npackets')
		stat_incr('nbytes', record_batch.size)
		stat_incr('nrecords', record_batch.length)

		record_batch.length, record_batch.size, record_batch.lock = 0, 0
	end, record_batch)

	return get_free_record_batch(self)
end

function _M:add(data, size)
	local timestamp_delta, record_batch, err = 0, get_free_record_batch(self)

	if not record_batch then
		return nil, err
	end

	size = size or #data

	record[7], size = data, size
	record[6], size = encode.varint(size, size)
	record[4], size = encode.varint(record_batch.length, size)
	record[3], size = encode.varint(timestamp_delta, size)
	-- header_length
	record[8], size = char(0x00), size + 1
	-- size + key_length + attributes
	record[1], size = encode.varint(size + 2, size + 2)


	if size + record_batch.size >= self.batch_size then
		record_batch = self:send()

		if not record_batch then
			return nil, err
		end
	end

	if record_batch.length > 0 then
		timestamp_delta = ngx.now() - record_batch.first_timestamp
	else
		zstd = zstd or zstandard.new(self.zstd_compression_level)
		timestamp_delta, record_batch.first_timestamp = 0, ngx.now()
	end

	record_batch.max_timestamp = record_batch.first_timestamp + timestamp_delta
	record_batch.length = record_batch.length + 1
	record_batch.size = record_batch.size + size

	local idx = record_batch.length + self.offset
	record_batch[idx], record_batch[idx + 1] = zstd:update(concat(record))

	return record_batch.length >= self.batch_length and self:send()
end

return _M
