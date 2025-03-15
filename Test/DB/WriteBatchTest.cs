using Core.DB;
using Test.Common;

namespace Test;

public class WriteBatchTest
{


    [Test]
    public void TestWriteBatch()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var engine = new Engine(engineOps);
        
        // 
        var option = new WriteBatchOptions()
        {
            MaxBatchSize = 1_000,
            SyncWhenCommited = true,
        };
        var batch = engine.CreateWriteBatch(option);
        
        for (int i = 0; i < 1_000; i++)
        {
            batch.Put(KeyValueHelper.GetKey(i),KeyValueHelper.GetValue(i));
        }
        batch.Commit();
        
        Assert.AreEqual(1_000,engine.GetKeys().Count());
        Assert.AreEqual(2,engine.TransactionNumber);
        engine.Dispose();
        
        
        // 重启
        engine = new Engine(engineOps);
        Assert.AreEqual(1_000,engine.GetKeys().Count());
        Assert.AreEqual(2,engine.TransactionNumber);
        
        batch = engine.CreateWriteBatch(option);
        
        for (int i = 1_000; i < 2_000; i++)
        {
            batch.Put(KeyValueHelper.GetKey(i),KeyValueHelper.GetValue(i));
        }
        batch.Commit();
        
        Assert.AreEqual(2_000,engine.GetKeys().Count());
        Assert.AreEqual(3,engine.TransactionNumber);
        
        engine.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestWriteBatchDelete()
    {
        
    }
    
}