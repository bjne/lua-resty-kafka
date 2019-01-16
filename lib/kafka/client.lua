local metadata = require "kafka.metadata"
local decode = require "kafka.decode"

local tcp = ngx.socket.tcp
local lshift = bit.lshift
local bor = bit.bor
local byte = string.byte
local lower = string.lower
local concat = table.concat
local int32 = decode.int32

local _M = {}
local mt = { __index = _M }
local etc_hosts = {}

for line in io.lines("/etc/hosts") do
	local ip, hosts = string.match(line, '(%d+%.%d+%.%d+%.%d+)%s+(.*)$')
	string.gsub(hosts or '', '(%S+)', function(host)
		etc_hosts[lower(host)] = ip
	end)
end

local function _request(pkt, host, port)
	local sock, data, err = tcp()

	data, err = sock:connect(etc_hosts[lower(host)] or host, port)
	if not data then
		return nil, err
	end

	data, err = sock:send(pkt)
	if not data then
		return nil, err
	end

	data, err = sock:receive(4)
	if not data then
		return nil, err
	end

	data, err = sock:receive(int32(byte(data, 1, 4)))
	if not data then
		return nil, err
	end

	return data, sock:setkeepalive(10)
end

function _M.new(config)
	if config.broker_list and type(config.broker_list) ~= "table" then
		return nil, "broker_list is not table"
	end

	local client, err = {
		client_id = config.client_id or 'ngx',
		broker_list = config.broker_list or {{host = "127.0.0.1", port = 9092}}
	}

	return setmetatable(client, mt)
end

function _M:send(pkt, topic, ...)
	local broker_list, response, err = topic == nil and self.broker_list

	for _,broker in ipairs(broker_list or metadata(self, topic, ...)) do
		response, err = _request(pkt, broker.host, broker.port)
		if response then
			return response
		end
	end

	return nil, err or "no valid host found"
end

return _M
