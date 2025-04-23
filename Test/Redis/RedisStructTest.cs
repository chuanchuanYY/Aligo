using Core.DB;
using Redis;

namespace Test.Redis;

public class RedisStructTest
{
    [Test]
    public void TestSet()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var setResult1 = redis.Set("key1", "value1");
        Assert.NotNull(setResult1);
        
        var setResult2 = redis.Set("key2", "value2",TimeSpan.FromSeconds(2));
        Assert.NotNull(setResult2);
      
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestGet()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var setResult1 = redis.Set("key1", "value1");
        Assert.NotNull(setResult1);
        
        var setResult2 = redis.Set("key2", "value2",TimeSpan.FromSeconds(2));
        Assert.NotNull(setResult2);
        
        var setResult3 = redis.Set("key3", "value3",TimeSpan.FromSeconds(100));
        Assert.NotNull(setResult3);

        var getResult1 = redis.Get("key1");
        Assert.NotNull(getResult1);
        
        Task.Delay(2000).Wait(); // wait timeout 
        var getResult2 = redis.Get("key2");
        Assert.IsNull(getResult2);
        
        var getResult3 = redis.Get("key3");
        Assert.IsNotNull(getResult3);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }


    [Test]
    public void TestDel()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var setResult1 = redis.Set("key1", "value1");
        Assert.NotNull(setResult1);
        
        var setResult2 = redis.Set("key2", "value2");
        Assert.NotNull(setResult2);
        
        
        // delete 
        Assert.DoesNotThrow(() => redis.Del("key1"));
        
        // get 
        var getResult1 = redis.Get("key1");
        Assert.IsNull(getResult1);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestGetType()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var setResult1 = redis.Set("key1", "value1");
        Assert.NotNull(setResult1);
        
        var setResult2 = redis.Set("key2", "value2",TimeSpan.FromSeconds(2));
        Assert.NotNull(setResult2);

        var type = redis.GetType("key1");
        Assert.AreEqual(Types.String,type);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }
}