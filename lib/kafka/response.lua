local gsub = string.gsub
local match = string.match
local insert = table.insert
local concat = table.concat

local _M = {}

local default_header = {"int32:correlation_id"}

local function generate_parser(...)
	local indent, tab = -1, "\t"

	local code = {[[
		local sub = string.sub
		local gsub = string.gsub
		local byte = string.byte
		local tohex = bit.tohex

		return function(data)
		local t, pos, len, size = {}, 1
		local int = function(size)
			pos = pos + size / 8
			return tonumber(gsub(sub(data, pos - size / 8, pos - 1), '.',
				function(b)
					return tohex(byte(b), 2)
				end
			), 16)
		end

		local boolean = function()
			pos, size = pos + 1, byte(data, pos)
			return size == 1 and true or false
		end

		local string = function()
			size = int(16)
			if size < 0xffff then
				pos = pos + size
				return sub(data, pos - size, pos - 1)
			end
		end
	]]}

	local _ = function(...)
		insert(code, tab:rep(indent) .. concat{...})
	end

	local function recurse(t)
		local k,v
		indent = indent + 1
		while true do
			k, v = next(t, k)
			if not k then break end

			local typ, nam = match(v, '^([^:]+):(.+)$')
			if typ == 'array' then
				_("t['", nam, "']={}")
				_("for i=1,int(32) do")
				_(tab, "local _t,t=t['", nam, "'],{}")
				k,v = next(t, k)
				recurse(v)
				indent = indent - 1
				_(tab, "_t[i]=t")
				_("end")
			else
				typ = gsub(typ, '%d*$', '(%1)', 1)
				_("t['", nam, "']=", typ)
			end
		end
	end

	for i=1, select('#', ...) do
		recurse((select(i, ...)))
	end

	_("return t ; end")

	return loadstring(concat(code, "\n"))()
end

function _M.new(...)
	return generate_parser(default_header, ...)
end

return _M
