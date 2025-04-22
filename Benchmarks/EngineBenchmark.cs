using System.Buffers.Binary;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using Core.DB;

namespace Benchmarks;

[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[Orderer(SummaryOrderPolicy.FastestToSlowest)] // 明确执行顺序
public class EngineBenchmark
{
    private Engine _engine;
    private Engine _getEngin;
    private byte[][] _keys;
    private byte[][] _values;
    
    [Params(1000, 10_000,100_000,1_000_000)]
    public int DataCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        // var dir = Path.Join(Environment.CurrentDirectory, $"BenchmarkData");
        // Directory.CreateDirectory(dir);
        // _engine = new Engine(new EngineOptions(dir));
        //
        // var getDir  = Path.Join(Environment.CurrentDirectory, $"BenchmarkData");
        // Directory.CreateDirectory(dir);
        // _getEngin = new Engine(new EngineOptions(dir));
        _keys = new byte[DataCount][];
        _values = new byte[DataCount][];
        // 生成唯一测试数据（使用递增整数作为键）
        for (int i = 0; i < DataCount; i++)
        {
            _keys[i] = Encoding.UTF8.GetBytes("key" + i);
            _values[i] = Encoding.UTF8.GetBytes($"Value_{i:D10}");
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        var dir = Path.Join(Environment.CurrentDirectory, $"BenchmarkData");
        Directory.CreateDirectory(dir);
        _engine = new Engine(new EngineOptions(dir));
        
        var getDir  = Path.Join(Environment.CurrentDirectory, $"BenchmarkData_get");
        Directory.CreateDirectory(dir);
        _getEngin = new Engine(new EngineOptions(dir));
        
        // 提前插入数据
        for (int i = 0; i < DataCount; i++) 
        {
            _getEngin.Put(_keys[i], _values[i]);
        }
    }
    [Benchmark]
    [BenchmarkCategory("put")]
    public void Put()
    {
        for (int i = 0; i < DataCount; i++) // 每次迭代从头开始
        {
            _engine.Put(_keys[i], _values[i]);
        }
    }
    
    [Benchmark]
    [BenchmarkCategory("get")]
    public void SequentialGet()
    {
        for (int i = 0; i < DataCount; i++)
        {
            var result = _getEngin.Get(_keys[i]);
            // 黑盒处理避免JIT优化
            _ = result.Length;
        }
    }

    [IterationCleanup]
    public void GlobalCleanup()
    {
        _engine.Dispose();
        _getEngin.Dispose();
        
        // 清理当前迭代的数据库文件
        var dir = Path.Join(Environment.CurrentDirectory, $"BenchmarkData");
        var dirGet = Path.Join(Environment.CurrentDirectory, $"BenchmarkData_get");
        if (Directory.Exists(dir))
            Directory.Delete(dir, true);
        if (Directory.Exists(dirGet))
            Directory.Delete(dirGet, true);
    }
}