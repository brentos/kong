local lpack = require "lua_pack"
local protoc = require "protoc"
local pb = require "pb"
local pl_path = require "pl.path"
local date = require "date"

local bpack = lpack.pack
local bunpack = lpack.unpack


local grpc = {}

local NANOS_PER_SEC = 1000000000
date.ticks(NANOS_PER_SEC)

local function safe_set_type_hook(type, dec, enc)
  if not pcall(pb.hook, type) then
    ngx.log(ngx.NOTICE, "no type '" .. type .. "' defined")
    return
  end

  if not pb.hook(type) then
    pb.hook(type, dec)
  end

  if not pb.encode_hook(type) then
    pb.encode_hook(type, enc)
  end
end

local function set_hooks()
  pb.option("enable_hooks")

  safe_set_type_hook(
    ".google.protobuf.Timestamp",
     grpc.timestamp_to_string,
     grpc.parse_timestamp)

  safe_set_type_hook(
    ".google.protobuf.Duration",
    grpc.duration_to_string,
    grpc.parse_duration)
end

--- loads a .proto file optionally applies a function on each defined method.
function grpc.each_method(fname, f, recurse)
  local dir = pl_path.splitpath(pl_path.abspath(fname))
  local p = protoc.new()
  p:addpath("/usr/include")
  p:addpath("/usr/local/opt/protobuf/include/")
  p:addpath("/usr/local/kong/lib/")
  p:addpath("kong")
  p:addpath("kong/include")
  p:addpath("spec/fixtures/grpc")

  p.include_imports = true
  p:addpath(dir)
  p:loadfile(fname)
  set_hooks()
  local parsed = p:parsefile(fname)

  if f then

    if recurse and parsed.dependency then
      if parsed.public_dependency then
        for _, dependency_index in ipairs(parsed.public_dependency) do
          local sub = parsed.dependency[dependency_index + 1]
          grpc.each_method(sub, f, true)
        end
      end
    end

    for _, srvc in ipairs(parsed.service or {}) do
      for _, mthd in ipairs(srvc.method or {}) do
        f(parsed, srvc, mthd)
      end
    end
  end

  return parsed
end


--- wraps a binary payload into a grpc stream frame.
function grpc.frame(ftype, msg)
  return bpack("C>I", ftype, #msg) .. msg
end

--- unwraps one frame from a grpc stream.
--- If success, returns `content, rest`.
--- If heading frame isn't complete, returns `nil, body`,
--- try again with more data.
function grpc.unframe(body)
  if not body or #body <= 5 then
    return nil, body
  end

  local pos, ftype, sz = bunpack(body, "C>I")       -- luacheck: ignore ftype
  local frame_end = pos + sz - 1
  if frame_end > #body then
    return nil, body
  end

  return body:sub(pos, frame_end), body:sub(frame_end + 1)
end

-- convert duration table: {seconds: 1, nanos: 123456}
-- to string: 1.000123456s
function grpc.duration_to_string(t)
    if type(t) ~= "table" then
      error(string.format("expected table, got (%s)%q", type(t), tostring(t)))
    end

    local result
    local seconds = t.seconds
    local nanos = t.nanos
    local sign_char = ""
    if t.seconds < 0 or t.nanos < 0 then
      sign_char = "-"
      seconds = -t.seconds
      nanos = -t.nanos
    end

    result = sign_char .. seconds
    if nanos ~= 0 then
      result = result .. "." .. grpc.format_nanos(nanos)
    end

    result = result .. "s"
    -- TODO: normalize to 3, 6, or 9 digits
    return result
end

-- parse a duration as a string (1.000123456s)
-- return a duration table {seconds: 1, nanos: 123456}
function grpc.parse_duration(t)
    if type(t) ~= "string" then
      error (string.format("expected duration string, got (%s)%q", type(t), tostring(t)))
    end

    if t and string.sub(t,-1) ~= 's' then
      error (string.format("invalid duration string: %q", t))
    end

    local negative = false;
    if string.sub(t,1) == '-' then
      negative = true
      t = string.sub(t,2,#t)
    end

    -- drop 's'
    local seconds = string.sub(t, 1, #t-1)
    local nanos = 0
    local decimalPos = string.find(seconds, "%.")

    if decimalPos then
      nanos = tonumber(string.sub(seconds, decimalPos+1, #seconds))
      seconds = string.sub(seconds, 1, decimalPos-1)
    end

    seconds = tonumber(seconds)

    if negative then
      return {
        ["seconds"] = -seconds,
        ["nanos"] = -nanos,
      }
    else
      return {
        ["seconds"] = seconds,
        ["nanos"] = nanos,
      }
    end
end

function grpc.timestamp_to_string(t)
  if type(t) ~= "table" then
    error(string.format("expected table, got (%s)%q", type(t), tostring(t)))
  end

  --return date(t.seconds):fmt("${iso}")
  local seconds_date = date(t.seconds):fmt("${iso}") -- up to second precision

  if t.nanos ~= 0 then
    seconds_date = seconds_date .. "." .. grpc.format_nanos(t.nanos)
  end

  return seconds_date .. "Z"

end

function grpc.parse_timestamp(t)

  -- TODO: remove pretty print
  local pretty = require("pl.pretty")

  if type(t) ~= "string" then
    error (string.format("expected time string, got (%s)%q", type(t), tostring(t)))
  end

  local day_offset = string.find(t, "T")
  if not day_offset then
    error (string.format("failed to parse timestamp: invalid timestamp: %s", tostring(t)))
  end

  local tz_offset_position = string.find(t, "Z", day_offset)
  print(tz_offset_position)
  if not tz_offset_position then
    tz_offset_position = string.find(t, "+", day_offset)
  end

  if not tz_offset_position then
    tz_offset_position = string.find(t, "-", day_offset)
  end

  if not tz_offset_position then
    error("failed to parse timestamp: missing valid timezone offset")
  end
  print(tz_offset_position)
  local time_value = string.sub(t,1, tz_offset_position-1)
  print(time_value)
  local seconds = time_value
  local nanos = 0
  local point_position = string.find(time_value,"%.")
  print(point_position)

  if point_position then
    seconds = string.sub(time_value, 1, point_position-1)
    print(seconds)
    nanos = string.sub(time_value, point_position+1)
    print(nanos)
  end

  local date_value = date(seconds)
  pretty(date_value)
  print(date_value:fmt("${iso}"))
  seconds = date_value - date.epoch()
  seconds = seconds:spanseconds()
  print(seconds)

  -- Parse timezone offsets
  if string.sub(t, tz_offset_position, tz_offset_position) == "Z" then
    if #t ~= tz_offset_position then
      error("failed to parse timestamp: invalid trailing data")
    end
  else
    local tz_offset = string.sub(t, tz_offset_position+1)
    print(tz_offset)

    local pos = string.find(tz_offset, ":")
    if not pos then
      error("failed to parse timestamp: invalid offset value")
    end
    local hours = string.sub(tz_offset, 1, pos-1)
    print(hours)
    local minutes = string.sub(tz_offset, pos+1)
    print(minutes)
    tz_offset = (hours*60+minutes)*60

    if string.sub(t, tz_offset_position, tz_offset_position) == "+" then
      seconds = seconds - tz_offset
    else
      seconds = seconds + tz_offset
    end
  end

  return {
    ["seconds"] = tonumber(seconds),
    ["nanos"] = tonumber(nanos),
  }

end

-- nanos should be either 0, 3, 6, or 9 digits
function grpc.format_nanos(nanos)

  local padded_nanos = string.format("%09d",nanos)
  if nanos % 1000000 == 0 then
    return string.sub(padded_nanos,1,3)
  elseif nanos % 1000 == 0 then
    return string.sub(padded_nanos,1,6)
  else
    return padded_nanos
  end

end


--// Special-case wrapper types.
--WellKnownTypePrinter wrappersPrinter =
--new WellKnownTypePrinter() {
--@Override
--public void print(PrinterImpl printer, MessageOrBuilder message) throws IOException {
--printer.printWrapper(message);
--}
--};
--printers.put(BoolValue.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(Int32Value.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(UInt32Value.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(Int64Value.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(UInt64Value.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(StringValue.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(BytesValue.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(FloatValue.getDescriptor().getFullName(), wrappersPrinter);
--printers.put(DoubleValue.getDescriptor().getFullName(), wrappersPrinter);

--private void mergeWrapper(JsonElement json, Message.Builder builder)
--throws InvalidProtocolBufferException {
--Descriptor type = builder.getDescriptorForType();
--FieldDescriptor field = type.findFieldByName("value");
--if (field == null) {
--throw new InvalidProtocolBufferException("Invalid wrapper type: " + type.getFullName());
--}
--builder.setField(field, parseFieldValue(field, json, builder));
--}


--private Object parseFieldValue(FieldDescriptor field, JsonElement json, Message.Builder builder)
--throws InvalidProtocolBufferException {
--if (json instanceof JsonNull) {
--if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
--&& field.getMessageType().getFullName().equals(Value.getDescriptor().getFullName())) {
--// For every other type, "null" means absence, but for the special
--// Value message, it means the "null_value" field has been set.
--Value value = Value.newBuilder().setNullValueValue(0).build();
--return builder.newBuilderForField(field).mergeFrom(value.toByteString()).build();
--} else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM
--&& field.getEnumType().getFullName().equals(NullValue.getDescriptor().getFullName())) {
--// If the type of the field is a NullValue, then the value should be explicitly set.
--return field.getEnumType().findValueByNumber(0);
--}
--return null;
--} else if (json instanceof JsonObject) {
--if (field.getType() != FieldDescriptor.Type.MESSAGE
--&& field.getType() != FieldDescriptor.Type.GROUP) {
--// If the field type is primitive, but the json type is JsonObject rather than
--// JsonElement, throw a type mismatch error.
--throw new InvalidProtocolBufferException(
--String.format("Invalid value: %s for expected type: %s", json, field.getType()));
--}
--}
--switch (field.getType()) {
--case INT32:
--case SINT32:
--case SFIXED32:
--return parseInt32(json);
--
--case INT64:
--case SINT64:
--case SFIXED64:
--return parseInt64(json);
--
--case BOOL:
--return parseBool(json);
--
--case FLOAT:
--return parseFloat(json);
--
--case DOUBLE:
--return parseDouble(json);
--
--case UINT32:
--case FIXED32:
--return parseUint32(json);
--
--case UINT64:
--case FIXED64:
--return parseUint64(json);
--
--case STRING:
--return parseString(json);
--
--case BYTES:
--return parseBytes(json);
--
--case ENUM:
--return parseEnum(field.getEnumType(), json);
--
--case MESSAGE:
--case GROUP:
--if (currentDepth >= recursionLimit) {
--throw new InvalidProtocolBufferException("Hit recursion limit.");
--}
--++currentDepth;
--Message.Builder subBuilder = builder.newBuilderForField(field);
--merge(json, subBuilder);
----currentDepth;
--return subBuilder.build();
--
--default:
--throw new InvalidProtocolBufferException("Invalid field type: " + field.getType());
--}
--}
--}

return grpc
