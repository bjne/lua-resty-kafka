local band = bit.band
local bor = bit.bor
local bxor = bit.bxor
local rshift  = bit.rshift
local lshift  = bit.lshift
local arshift = bit.arshift
local char = string.char

local _M = { int8 = char }

_M.varint = function(n, size)
	local function recurse(n)
		size = size + 1
		if n >= 128 or n <= -127 then
			return (bor(band(n, 0x7f), 0x80)), recurse(rshift(n, 7))
		else
			return band(n, 0x7f)
		end
	end

	return char(recurse(bxor(lshift(n, 1), arshift(n, 31)))), size
end

_M.varintl = function(n) -- long varint
	return bxor(lshift(n, 1), arshift(n, 63))
end

_M.int16 = function(n)
	return char(
		band(rshift(n, 8), 0xff),
		band(n, 0xff)
	)
end

_M.int32 = function(n)
	return char(
		band(rshift(n, 24), 0xff),
		band(rshift(n, 16), 0xff),
		band(rshift(n,  8), 0xff),
		band(n, 0xff)
	)
end

_M.int64 = function(n)
	return char(
		tonumber(band(rshift(n, 56), 0xff)),
		tonumber(band(rshift(n, 48), 0xff)),
		tonumber(band(rshift(n, 40), 0xff)),
		tonumber(band(rshift(n, 32), 0xff)),
		tonumber(band(rshift(n, 24), 0xff)),
		tonumber(band(rshift(n, 16), 0xff)),
		tonumber(band(rshift(n, 8), 0xff)),
		tonumber(band(n, 0xff))
	)
end

_M.string = function(s)
	return (not s or #s == 0) and _M.int16(-1) or (_M.int16(#s) .. s)
end

return _M
