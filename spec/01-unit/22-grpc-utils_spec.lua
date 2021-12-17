local grpc_tools = require "kong.tools.grpc"
local pretty = require "pl.pretty"

describe("grpc tools", function()
  it("visits service methods", function()
    local methods = {}
    grpc_tools.each_method("helloworld.proto",
      function(parsed, service, method)
        methods[#methods + 1] = string.format("%s.%s", service.name, method.name)
      end)
    assert.same({
      "HelloService.SayHello",
      "HelloService.UnknownMethod",
    }, methods)
  end)

  it("visits imported methods", function()
    local methods = {}
    grpc_tools.each_method("direct_imports.proto",
      function(parsed, service, method)
        methods[#methods + 1] = string.format("%s.%s", service.name, method.name)
      end, true)
    assert.same({
      "HelloService.SayHello",
      "HelloService.UnknownMethod",
      "Own.Open",
    }, methods)
  end)

  it("imports recursively", function()
    local methods = {}
    grpc_tools.each_method("second_level_imports.proto",
      function(parsed, service, method)
        methods[#methods + 1] = string.format("%s.%s", service.name, method.name)
      end, true)
    assert.same({
      "HelloService.SayHello",
      "HelloService.UnknownMethod",
      "Own.Open",
      "Added.Final",
    }, methods)
  end)

  it("converts duration to string", function()
    local result = grpc_tools.duration_to_string({seconds=1234,nanos=10101})
    assert.are.equal("1234.000010101s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=1})
    assert.are.equal("1234.000000001s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=10})
    assert.are.equal("1234.000000010s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=100})
    assert.are.equal("1234.000000100s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=1000})
    assert.are.equal("1234.000001s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=10000})
    assert.are.equal("1234.000010s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=100000})
    assert.are.equal("1234.000100s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=1000000})
    assert.are.equal("1234.001s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=10000000})
    assert.are.equal("1234.010s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=100000000})
    assert.are.equal("1234.100s", result)
    result = grpc_tools.duration_to_string({seconds=1234,nanos=0})
    assert.are.equal("1234s", result)
    result = grpc_tools.duration_to_string({seconds=0,nanos=1})
    assert.are.equal("0.000000001s", result)
  end)
  it("converts string to duration", function()
    local result = grpc_tools.parse_duration("1234.000010101s")
    assert.are.same({ seconds=1234,nanos=10101}, result)
    result = grpc_tools.parse_duration("1234.000101010s")
    assert.are.same({ seconds=1234,nanos=101010}, result)
  end)
  it("converts timestamp to string", function()
    local result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 1001001 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 1001 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 1 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 10 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 100 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 1000 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 10000 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 100000 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 1000000 })
    print(result)
    result = grpc_tools.timestamp_to_string({ seconds = 1639683581, nanos = 10000000 })
    print(result)

    -- Negative is a FAIL
    result = grpc_tools.timestamp_to_string({ seconds = -1639683581, nanos = 100000000 })
    print(result)
  end)
  it("converts string to timestamp", function()

    local result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.000001001Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.000000001Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.000000010Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.000000100Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.000001Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.000010Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.000100Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.001Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('2021-12-16T19:39:41.010Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('1918-01-16T04:20:19.100Z')
    pretty(result)
    result = grpc_tools.parse_timestamp('1918-01-16T04:20:19.100+04:00')
    pretty(result)
    result = grpc_tools.parse_timestamp('1918-01-16T04:20:19.100-04:00')
    pretty(result)
  end)
end)
