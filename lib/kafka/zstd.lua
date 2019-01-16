-- Copyright (C) by Bjørnar Ness <bjornar.ness@gmail.com>

local ffi = require "ffi"
local new_tab = require "table.new"
local clear_tab = require "table.clear"

local C = ffi.C
local ffi_str = ffi.string
local concat = table.concat

ffi.cdef[[
typedef struct ZSTD_inBuffer_s {
	const void* src;    /* start of input buffer */
	size_t size;        /* size of input buffer */
	size_t pos;         /* position where reading stopped. Will be updated. */
} ZSTD_inBuffer;

typedef struct ZSTD_outBuffer_s {
	void*  dst;         /* start of output buffer */
	size_t size;        /* size of output buffer */
	size_t pos;         /* position where writing stopped. Will be updated. */
} ZSTD_outBuffer;

typedef struct ZSTD_CStream_s ZSTD_CStream;

ZSTD_CStream* ZSTD_createCStream(void);

size_t ZSTD_CStreamOutSize(void);
size_t ZSTD_initCStream(ZSTD_CStream* zcs, int compressionLevel);
size_t ZSTD_freeCStream(ZSTD_CStream* zcs);
size_t ZSTD_compressStream(ZSTD_CStream* zcs,
                           ZSTD_outBuffer* output, ZSTD_inBuffer* input);
size_t ZSTD_endStream(ZSTD_CStream* zcs, ZSTD_outBuffer* output);
size_t ZSTD_resetCStream(ZSTD_CStream* zcs, unsigned long long pledgedSrcSize);

unsigned    ZSTD_isError(size_t code);
const char* ZSTD_getErrorName(size_t code);
]]

local uint8_t = ffi.typeof("uint8_t[?]")
local zstd_inbuffer = ffi.typeof("ZSTD_inBuffer[1]")
local zstd_outbuffer = ffi.typeof("ZSTD_outBuffer[1]")

local zstd = ffi.load("zstd")

local _M = {}
local mt = { __index = _M }

local buffer = new_tab(20, 0)

function _M.new(compression_level)
	local cstream = ffi.gc(zstd.ZSTD_createCStream(), ZSTD_freeCStream);
	if not cstream then
		return nil, "ZSTD_createCStream()"
	end

	local res = zstd.ZSTD_initCStream(cstream, compression_level or 1)
	if zstd.ZSTD_isError(res) ~= 0 then
		return nil, zstd.ZSTD_getErrorName(res)
	end

	local buf_size = zstd.ZSTD_CStreamOutSize()

	return setmetatable({
		cstream = cstream,
		buf = uint8_t(buf_size),
		buf_size = buf_size
	}, mt)
end

function _M:update(data, size)
	local cstream, output, input = self.cstream, zstd_outbuffer()
	local buf, buf_size, it, rlen = self.buf, self.buf_size, 0

	if data then
		data = type(data) == "table" and concat(data) or data
		input = zstd_inbuffer()
		input[0].src, input[0].size, input[0].pos = data, size or #data, 0
	end

	local maby_reset_stream = function()
		if input == nil then
			table.clear(buffer)
			zstd.ZSTD_resetCStream(cstream, 0)
		end
	end

	repeat
		output[0].dst, output[0].size, output[0].pos = buf, buf_size, 0

		rlen, it = input and zstd.ZSTD_compressStream(cstream, output, input)
			or zstd.ZSTD_endStream(cstream, output), it + 1
		
		if rlen < 0 and zstd.ZSTD_isError(rlen) then
			return nil, ZSTD_getErrorName(rlen), maby_reset_stream()
		end

		if it == 1 and (input and (input[0].pos == input[0].size) or rlen == 0) then
			return ffi_str(buf, output[0].pos), maby_reset_stream()
		end

		buffer[it], buffer[it + 1] = ffi_str(buf, output[0].pos)
	until input and (input[0].pos < input[0].size) or rlen == 0

	return concat(buffer), maby_reset_stream()
end

_M.finalize = _M.update

return _M
