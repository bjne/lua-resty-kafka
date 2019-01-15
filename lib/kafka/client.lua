local metadata = require "kafka.metadata"

local tcp = ngx.socket.tcp
local byte = string.byte
local lshift = bit.lshift
local bor = bit.bor

local _M = {}
local mt = { __index = _M }

local function _request(pkt, host, port)
	local sock, data, err = tcp()

	-- TODO: remove
	if host == "roadwarrior.hprt.no" then
		host = "127.0.0.1"
	end

	data, err = sock:connect(host, port)
	if not data then
		return nil, err
	end

	data, err = sock:send(pkt)
	if not data then
		ngx.log(ngx.ERR, err)
	end

	data, err = sock:receive(4)
	if not data then
		return nil, err
	end

	local a,b,c,d = byte(data, 1, 4)

	data, err = sock:receive(bor(lshift(a, 24), lshift(b, 16), lshift(c, 8), d))
	if not data then
		return nil, err
	end

	return data, sock:setkeepalive(10)
end

function _M.new(config)
	config.broker_list = config.broker_list or {
		{host = "127.0.0.1", port = 9092}
	}

	if type(config.broker_list) ~= "table" then
		return nil, "broker_list is not table"
	end

	local client, err = { broker_list = config.broker_list }

	client.metadata, err = metadata.new(config, client)
	if not client.metadata then
		return nil, err
	end

	return setmetatable(client, mt)
end

function _M:send(pkt, partition, refresh)
	local broker_list, response, err = partition == nil and self.broker_list

	for _,broker in ipairs(broker_list or self.metadata(partition, refresh)) do
		response, err = _request(pkt, broker.host, broker.port)
		if response then
			return response
		end
	end

	return nil, err or "no valid host found"
end

return _M