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

    [Test]
    public void TestHashSet()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var setResult1 = redis.HashSet("MySet", "a", "10");
        Assert.IsTrue(setResult1);
        
        var setResult2 = redis.HashSet("MySet", "b", "100");
        Assert.IsTrue(setResult2);
        
        var setResult3 = redis.HashSet("MySet", "a", "1");
        Assert.IsTrue(setResult3);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestHashGet()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var setResult1 = redis.HashSet("MySet", "a", "10");
        Assert.IsTrue(setResult1);
        
        var setResult2 = redis.HashSet("MySet", "b", "100");
        Assert.IsTrue(setResult2);
        
        var setResult3 = redis.HashSet("MySet", "a", "1");
        Assert.IsTrue(setResult3);
        
        // get
        var getResult1 = redis.HashGet("MySet","a");
        Assert.AreEqual("1", getResult1);
        
        var getResult2 = redis.HashGet("MySet","b");
        Assert.AreEqual("100", getResult2);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestHashDel()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var setResult1 = redis.HashSet("MySet", "a", "10");
        Assert.IsTrue(setResult1);
        
        var setResult2 = redis.HashSet("MySet", "b", "100");
        Assert.IsTrue(setResult2);
        
        var setResult3 = redis.HashSet("MySet", "a", "1");
        Assert.IsTrue(setResult3);
        
        // Del 
        var delResult1 = redis.HashDel("MySet", "a");
        Assert.AreEqual("1", delResult1);
        
        // get
        var getResult1 = redis.HashGet("MySet","a");
        Assert.IsNull(getResult1);
            
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestSAdd()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);
        
        var addResutl1 = redis.SAdd("MySet","a");
        Assert.IsTrue(addResutl1);
        
        // add same member
        var addResutl2 = redis.SAdd("MySet","a");
        Assert.IsTrue(addResutl2);
        
        var addResutl3 = redis.SAdd("MySet","b");
        Assert.IsTrue(addResutl3);
        
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }


    [Test]
    public void TestSRemove()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);
        
        var addResutl1 = redis.SAdd("MySet","a");
        Assert.IsTrue(addResutl1);
        
        // add same member
        var addResutl2 = redis.SAdd("MySet","a");
        Assert.IsTrue(addResutl2);
        
        var addResutl3 = redis.SAdd("MySet","b");
        Assert.IsTrue(addResutl3);


        var remResult1 = redis.SRemove("MySet", "a");
        Assert.IsTrue(remResult1);
        
        var remResult2 = redis.SRemove("MySet", "b");
        Assert.IsTrue(remResult2);
        
        // 
        var remResult3 = redis.SRemove("MySet", "a");
        Assert.IsFalse(remResult3);
        
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }

    [Test]
    public void TestSIsMember()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);
        
        var addResutl1 = redis.SAdd("MySet","a");
        Assert.IsTrue(addResutl1);
        
        // add same member
        var addResutl2 = redis.SAdd("MySet","a");
        Assert.IsTrue(addResutl2);
        
        var addResutl3 = redis.SAdd("MySet","b");
        Assert.IsTrue(addResutl3);
        
        
        var isMemberRes1 = redis.SIsMember("MySet","a");
        Assert.IsTrue(isMemberRes1);
        
        var isMemberRes2 = redis.SIsMember("MySet","b");
        Assert.IsTrue(isMemberRes2);
        
        var isMemberRes3 = redis.SIsMember("MySet","c");
        Assert.IsFalse(isMemberRes3);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }


    [Test]
    public void TestLPush()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir_LPush");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var pushResult1 = redis.LPush("LPush", "1");
        Assert.IsTrue(pushResult1);
        
        var pushResult2 = redis.LPush("LPush", "2");
        Assert.IsTrue(pushResult2);
        
        var pushResult3 = redis.LPush("LPush", "3");
        Assert.IsTrue(pushResult3);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }
    
    
    [Test]
    public void TestRPush()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir_RPush");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var pushResult1 = redis.RPush("RPush", "1");
        Assert.IsTrue(pushResult1);
        
        var pushResult2 = redis.RPush("RPush", "2");
        Assert.IsTrue(pushResult2);
        
        var pushResult3 = redis.RPush("RPush", "3");
        Assert.IsTrue(pushResult3);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }
    
    
    [Test]
    public void TestLPop()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir_LPop");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var pushResult1 = redis.LPush("LPush", "1");
        Assert.IsTrue(pushResult1);
        
        var pushResult2 = redis.LPush("LPush", "2");
        Assert.IsTrue(pushResult2);
        
        var pushResult3 = redis.LPush("LPush", "3");
        Assert.IsTrue(pushResult3);

        var popResult1 = redis.LPop("LPush");
        Assert.IsNotNull(popResult1);
        Assert.AreEqual("3", popResult1);
        
        var popResult2 = redis.LPop("LPush");
        Assert.IsNotNull(popResult2);
        
        var popResult3 = redis.LPop("LPush");
        Assert.IsNotNull(popResult3);
        
        // 
        var popResult4 = redis.LPop("LPush");
        Assert.IsNull(popResult4);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }
    
    [Test]
    public void TestRPop()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir_RPop");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var pushResult1 = redis.RPush("RPush", "1");
        Assert.IsTrue(pushResult1);
        
        var pushResult2 = redis.RPush("RPush", "2");
        Assert.IsTrue(pushResult2);
        
        var pushResult3 = redis.RPush("RPush", "3");
        Assert.IsTrue(pushResult3);

        var popResult1 = redis.RPop("RPush");
        Assert.IsNotNull(popResult1);
        Assert.AreEqual("3", popResult1);
        
        var popResult2 = redis.RPop("RPush");
        Assert.IsNotNull(popResult2);
        
        var popResult3 = redis.RPop("RPush");
        Assert.IsNotNull(popResult3);
        
        // 
        var popResult4 = redis.RPop("RPush");
        Assert.IsNull(popResult4);
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }
    
    [Test]
    public void TestZAdd()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir_ZAdd");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var addResult1 = redis.ZAdd("MyZSet", 100, "a");
        Assert.IsTrue(addResult1);
      
        var addResult2 = redis.ZAdd("MyZSet", 110, "a");
        Assert.IsTrue(addResult2);
        
        var addResult3 = redis.ZAdd("MyZSet", 200, "b");
        Assert.IsTrue(addResult3);
        
        
        redis.Dispose();
        Directory.Delete(dirPath,true);
    }
    
    [Test]
    public void TestZScore()
    {
        var dirPath = Environment.CurrentDirectory;
        dirPath = Path.Join(dirPath, "TestDir_ZScore");
        var engineOps = new EngineOptions(dirPath);
        var redis = new RedisStruct(engineOps);

        var addResult1 = redis.ZAdd("MyZSet", 100, "a");
        Assert.IsTrue(addResult1);
      
        var addResult2 = redis.ZAdd("MyZSet", 110, "a");
        Assert.IsTrue(addResult2);
        
        var addResult3 = redis.ZAdd("MyZSet", 200, "b");
        Assert.IsTrue(addResult3);
        
        var score1 = redis.ZScore("MyZSet","a");
        Assert.AreEqual(110,score1);
        
        var score2 = redis.ZScore("MyZSet","b");
        Assert.AreEqual(200,score2);

        redis.Dispose();
        Directory.Delete(dirPath,true);
    }
}