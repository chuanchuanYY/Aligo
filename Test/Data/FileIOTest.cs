using System.Text;
using Core.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Test;

public class FileIOTest
{
    private  ILoggerFactory _loggerFactory;
    [SetUp]
    public void Setup()
    {
        _loggerFactory = LoggerFactory.Create(b =>
        {
            b.Services.AddLogging();
        });
    }

    [TearDown]
    public void TearDown()
    {
        _loggerFactory.Dispose();
    }

    [Test]
    public void TestWriteReturnWriteSize()
    {
        string filePath = Path.GetTempFileName();
        try
        {
            IIOManager io = new FileIO(filePath,_loggerFactory.CreateLogger<FileIO>());
            var buf = Encoding.UTF8.GetBytes("some");
            var writeCount = io.Write(buf);
            Assert.IsTrue(writeCount == (ulong)buf.Length );
            (io as FileIO)!.Dispose();
        }
        finally
        {
            File.Delete(filePath);
        }
    }

    [Test]
    public void TestRead()
    {
        string filePath = Path.GetTempFileName();
        IIOManager? io = null;
        try
        {
            io = new FileIO(filePath,_loggerFactory.CreateLogger<FileIO>());
            var buf = Encoding.UTF8.GetBytes("some");
            io.Write(buf);
            byte[] readBuf = new byte[buf.Length];
            var readSize = io.Read(readBuf, 0);
            Assert.IsTrue(readSize == (ulong)readBuf.Length);
            Assert.IsTrue(Encoding.UTF8.GetString(readBuf).Equals("some"));
        }
        finally
        {
           if(io is FileIO fileIO)
           {
               fileIO.Dispose();
           }
            File.Delete(filePath);
        }
    }
}