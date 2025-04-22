using BenchmarkDotNet.Running;

namespace Benchmarks;

class Program
{
    static void Main(string[] args)
    {
        var putSummary = BenchmarkRunner.Run<EngineBenchmark>();
       
    }
}