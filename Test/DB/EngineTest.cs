using System.Collections;
using System.Diagnostics;
using System.Text;
using Core.DB;
using NUnit.Framework.Internal.Execution;
using Test.Common;

namespace Test;

public class EngineTest
{
    [Test]
    public void TestEnginePut()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var engine = new Engine(engineOps);

        var putKey1Res = engine.Put(KeyValueHelper.GetKey(1),KeyValueHelper.GetValue(1));
        Assert.True(putKey1Res);
        engine.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestStat()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var engine = new Engine(engineOps);
        var keyValues = Enumerable.Range(0, 100)
            .Select(n => (KeyValueHelper.GetKey(n), KeyValueHelper.GetValue(n)))
            .ToList();
        
        keyValues.ForEach(item =>
        {
            var putKeyRes = engine.Put(item.Item1,item.Item2);
            Assert.True(putKeyRes);
        });
        var keyValues2 = Enumerable.Range(0, 50)
            .Select(n => (KeyValueHelper.GetKey(n), KeyValueHelper.GetValue(n)))
            .ToList();
        
        keyValues2.ForEach(item =>
        {
            var putKeyRes = engine.Put(item.Item1,item.Item2);
            Assert.True(putKeyRes);
        });
        engine.Sync();
        var stat = engine.GetStat();
        
        Assert.AreEqual(100,stat.KeyNum);
        Assert.AreEqual(1,stat.DataFileNum);
        Assert.AreEqual(2230,stat.ReclaimableSize);
        Assert.AreEqual(6710,stat.DiskSize);
        
        engine.Dispose();
        Directory.Delete(dirPath,true); 
    }
    
    [Test]
    public void TestPutSameKey()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var engine = new Engine(engineOps);

        var putKey1Res = engine.Put(KeyValueHelper.GetKey(1),KeyValueHelper.GetValue(1));
        var putKey2Res = engine.Put(KeyValueHelper.GetKey(1),KeyValueHelper.GetValue(2));
        Assert.True(putKey1Res);
        Assert.True(putKey2Res);
        
        // value will be updated value
        var readUpdatedValue = engine.Get(KeyValueHelper.GetKey(1));
        Assert.IsTrue(StructuralComparisons.StructuralEqualityComparer.Equals(readUpdatedValue,KeyValueHelper.GetValue(2)));
        engine.Dispose();
        Directory.Delete(dirPath,true);
    }
    [Test]
    public void TestEnginePut2()
    {
        // 测试写入超过数据文件最大容量，以让它创建新的数据文件
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath,1024*10);
        var engine = new Engine(engineOps);
        int putCount = 10_000;  
        for (int i = 0; i < putCount; i++)
        {
            var putKey1Res = engine.Put(KeyValueHelper.GetKey(i),KeyValueHelper.GetValue(i));
            Assert.True(putKey1Res);
        }
        engine.Dispose();
        
        // 然后重启引擎
        engine = new Engine(engineOps);
        Console.WriteLine(engine.GetKeys().Count());
        Assert.AreEqual(putCount,engine.GetKeys().Count());
        
        engine.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestEngineGet()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var engine = new Engine(engineOps);
        
        engine.Put(KeyValueHelper.GetKey(1),KeyValueHelper.GetValue(1));
        var result =engine.Get(KeyValueHelper.GetKey(1));
        Assert.True(StructuralComparisons.StructuralEqualityComparer.Equals(result, KeyValueHelper.GetValue(1)));
        engine.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestEngineDelete()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var engine = new Engine(engineOps);
        
        engine.Put(KeyValueHelper.GetKey(1),KeyValueHelper.GetValue(1));
        var result =engine.Delete(KeyValueHelper.GetKey(1));
        Assert.True(result);
        engine.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestEngineGetKeys()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var engine = new Engine(engineOps);

        int putCount = 100;
        for (int i = 0; i < putCount; i++)
        {
            engine.Put(KeyValueHelper.GetKey(i),KeyValueHelper.GetValue(i));
        }
        Assert.True(engine.GetKeys().Count( ) == putCount);
        engine.Dispose();
        Directory.Delete(dirPath,true);
    }


    [Test]
    public void TestBackUp()
    {
        // 测试写入超过数据文件最大容量，以让它创建新的数据文件
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath,1024*10);
        var engine = new Engine(engineOps);
        int putCount = 10_000;  
        for (int i = 0; i < putCount; i++)
        {
            var putKey1Res = engine.Put(KeyValueHelper.GetKey(i),KeyValueHelper.GetValue(i));
            Assert.True(putKey1Res);
        }
        
        engine.BackUp(Path.Join(dirPath,"BackupDir"));
        engine.Dispose();
        Directory.Delete(Path.Join(dirPath,"BackupDir"),true);
        Directory.Delete(dirPath,true);
    }
}