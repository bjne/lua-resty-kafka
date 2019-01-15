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

local _M = { _VERSION = 0.1 }
local mt = { __index = _M }

local zstd -- must be initialized per worker (on add)
local record = { [2] = char(0x00), [5] = char(0x01) } -- attributes, key_len

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
		max_packets            = config.max_packets or 2,
		batch_max_size         = config.batch_max_size or 2000,
		batch_max_length       = config.batch_max_length or 50,
		required_acks          = config.required_acks or 1,
		timeout                = config.timeout or 1500,
		max_timeout            = config.max_timeout or 2000,
		zstd_compression_level = config.zstd_compression_level or 9,

		offset = 8
	}

	local request_header, produce_header, record_batch_header = {
		char(0x00, 0x00),                    -- api_key
		char(0x00, 0x07),                    -- api_version
		char(0,0,0,0),                       -- correlation_id
		encode.string(self.client_id)        -- client_id
	}, {
		encode.string(""),                   -- transaction_id
		encode.int16(self.required_acks),    -- required_acks
		encode.int32(self.timeout),          -- timeout
		encode.int32(1),                     -- number of topics
	}, {
		encode.int64(0),                     -- offset
		encode.int32(0),                     -- size in bytes
		encode.int32(-1),                    -- partition leader epoch
		char(0x02),                          -- magic
		encode.int32(0),                     -- crc32c
	}

	-- (request_header) + (produce_header)
	self.header_length = (10 + #self.client_id) + (26 + #self.topic)

	local record_batch_1, record_batch
	for i=1, self.max_packets do
		local r = new_tab(self.offset + self.batch_max_length + 1 + 1, 5)

		if not record_batch then
			record_batch, record_batch_1, self.record_batch = r, r, r
		else
			record_batch.next = r
			record_batch = record_batch.next

			record_batch.next = i == self.max_packets and record_batch_1
		end

		record_batch[1] = encode.int16(4)    -- attributes (zstd)
		record_batch[5] = encode.int64(-1)   -- producer_id
		record_batch[6] = encode.int16(-1)   -- producer_epoch
		record_batch[7] = encode.int32(-1)   -- first_sequence (not used)

		record_batch.size, record_batch.length, record_batch.pkt = 0, 0, {
			[01] = nil,                       -- size (int32),
			[02] = request_header,
			[03] = produce_header,
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

	return setmetatable(self, mt)
end


function _M:send()
	local record_batch = self.record_batch

	if record_batch.lock then -- TODO: possible to end here?
		ngx.log(ngx.ERR, "record_batch is locked:", record_batch.id)
		return nil, "record_batch is locked"
	end

	record_batch.lock = true

	record_batch[2] = encode.int32(record_batch.length - 1)
	record_batch[3] = encode.int64(record_batch.first_timestamp)
	record_batch[4] = encode.int64(record_batch.max_timestamp)
	record_batch[8] = encode.int32(record_batch.length)

	local pkt, data = record_batch.pkt, concat(record_batch)

	local size = #data + 9                    -- record batch header

	pkt[01] = encode.int32(size + 12 + self.header_length)
	pkt[06] = encode.int32(0)                 -- partition_id
	pkt[07] = encode.int32(size + 12)         -- messageset size
	pkt[09] = encode.int32(size)              -- record batch length
	pkt[12] = encode.int32(crc32c(data))
	pkt[13] = data

	ngx.timer.at(0, function(premature, record_batch)
		local data, err = self.client:send(record_batch.pkt, 0)
		if data then
			ngx.log(ngx.ERR, require"cjson".encode(response(data)))
		else
			ngx.log(ngx.ERR, err)
		end

		record_batch.length, record_batch.lock = 0, nil
	end, record_batch)

	record_batch = record_batch.next
	if record_batch.length > 0 then -- not empty
		if record_batch.first_timestamp + self.max_timeout <= ngx.now() then
			record_batch.length, record_batch.lock = 0, nil
		else
			return nil, "no free buffers"
		end
	end

	record_batch.size, self.record_batch = 0, record_batch

	return record_batch
end


function _M:add(data, size)
	local record_batch, timestamp_delta, err = self.record_batch, 0

	if record_batch.lock then -- TODO: iterate record_batch.next + timeout
		return nil, ngx.log(ngx.ERR, "FAILED ADDING DATA, record locked: ", record_batch.id)
	end

	size = size or #data

	if size + record_batch.size >= self.batch_max_size then
		record_batch, err = self:send()

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

	record[7], size = data, size
	record[6], size = encode.varint(size, size)
	record[4], size = encode.varint(record_batch.length, size)
	record[3], size = encode.varint(timestamp_delta, size)
	-- header_length
	record[8], size = char(0x00), size + 1
	-- size + key_length + attributes
	record[1], size = encode.varint(size + 2, size + 2)

	record_batch.max_timestamp = record_batch.first_timestamp + timestamp_delta
	record_batch.length = record_batch.length + 1

	local idx = record_batch.length + self.offset
	record_batch[idx], record_batch[idx + 1] = zstd:update(concat(record))

	if record_batch.length >= self.batch_max_length then
		record_batch[idx + 1], record_batch[idx + 2] = zstd:finalize()

		return self:send()
	end
end

return _M
