﻿using System.Text;
using Core.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Test;

public class FileIOTest
{
    [SetUp]
    public void Setup()
    {
       
    }

    [Test]
    public void TestWriteReturnWriteSize()
    {
        string filePath = Path.GetTempFileName();
        try
        {
            IIOManager io = new FileIO(filePath);
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
    public void TestWrite()
    {
        string filePath = Path.GetTempFileName();
        var start = DateTime.Now;
        try
        {
            IIOManager io = new FileIO(filePath);
            int count = 10_000; //
            for (int i = 0; i < count; i++)
            {
                var buf = "some"u8.ToArray();
                var writeCount = io.Write(buf);
                Assert.IsTrue(writeCount == (ulong)buf.Length );
            }
            
            Console.WriteLine(DateTime.Now - start);
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
        // 如果每次写入都Sync刷盘，速度会相当慢。
        string filePath = Path.GetTempFileName();
        IIOManager? io = null;
        try
        {
            io = new FileIO(filePath);
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