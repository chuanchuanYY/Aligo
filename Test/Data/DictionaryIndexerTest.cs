using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Core.Data;

namespace Test;
public class DictionaryIndexerTest
{
   
  
    [SetUp]
    public void Setup()
    {
     
    }


    [Test]
    public void TestPutKeyReturnTrue()
    {
        IIndexer indexer = new DictionaryIndexer();

        var put1Result = indexer.Put(Encoding.UTF8.GetBytes("key-1"), new LogRecordPos(1, 0));
        Assert.IsTrue(put1Result);
    }

    [Test] 
    public void TestPutExistKeyReturnTrueAndUpdateValue()
    {
        IIndexer indexer = new DictionaryIndexer();

        var put1Result = indexer.Put(Encoding.UTF8.GetBytes("key-1"), new LogRecordPos(1, 0));
        Assert.IsTrue(put1Result);
        
        // 插入重复key
        var put2Result = indexer.Put(Encoding.UTF8.GetBytes("key-1"), new LogRecordPos(2, 1));
        Assert.IsTrue(put2Result);

        var updated = indexer.Get(Encoding.UTF8.GetBytes("key-1"));
        Assert.IsNotNull(updated);
        Assert.IsTrue(updated.FileID == 2);
        Assert.IsTrue(updated.Offset == 1);
    }

    [Test]
    public void TestGetReturnLogRecordPos()
    {
        IIndexer indexer = new DictionaryIndexer();
        
        var put1Result = indexer.Put(Encoding.UTF8.GetBytes("key-1"), new LogRecordPos(1, 0));
        Assert.IsTrue(put1Result);
        
        var result = indexer.Get(Encoding.UTF8.GetBytes("key-1"));
        Assert.IsNotNull(result);
        Assert.IsTrue(result.FileID == 1);
        Assert.IsTrue(result.Offset == 0);
        
    }


    [Test]
    public void TestDeleteKeyReturnTrue()
    {
        IIndexer indexer = new DictionaryIndexer();
        
        var put1Res = indexer.Put(Encoding.UTF8.GetBytes("key-1"), new LogRecordPos(1, 0));
        var put2Res = indexer.Put(Encoding.UTF8.GetBytes("key-1"), new LogRecordPos(1, 0));
        Assert.IsTrue(put1Res);
        Assert.IsTrue(put2Res);

        var deleteRes = indexer.Delete(Encoding.UTF8.GetBytes("key-1"));
        Assert.IsTrue(deleteRes);
    }
}