-- Copyright (C) by Bjørnar Ness <bjornar.ness@gmail.com>

local ffi = require "ffi"

local cast = ffi.cast
local bxor = bit.bxor
local bnot = bit.bnot
local band = bit.band
local rshift = bit.rshift

local crc32_t = ffi.new('const uint32_t[256]', (function()
	local function init_lookup_table(crc)
		local iteration = crc

		for _=1,8 do
			crc = band(crc, 1) == 1
				and bxor(rshift(crc, 1), 0x82f63b78)
				 or rshift(crc, 1)
		end

		if iteration < 256 then
			return crc, init_lookup_table(iteration + 1)
		end
	end

	return init_lookup_table(0)
end)())

local function crc32c(buf, len, crc)
	len = len or #buf
	buf, crc = cast('const uint8_t*', buf), bnot(crc or 0)

	for i=0,len-1 do
		crc = bxor(rshift(crc, 8), crc32_t[bxor(crc % 256, buf[i])])
	end

	return bnot(crc)
end

assert(crc32c('123456789') == -486108541)

return crc32c
