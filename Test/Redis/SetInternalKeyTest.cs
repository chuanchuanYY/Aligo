using Redis;

namespace Test.Redis;

public class SetInternalKeyTest
{

    [Test]
    public void TestEncodeAndDecode()
    {
        var key = new SetInternalKey("key1",12345,"a");
        var encoded = key.Encode();
        var decodedKey = SetInternalKey.Decode(encoded);
        Assert.AreEqual("key1", decodedKey.key);
        Assert.AreEqual(12345, decodedKey.Version);
        Assert.AreEqual("a", decodedKey.Member);
    }
}