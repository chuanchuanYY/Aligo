using System.Text;

namespace Test.Common;

public class KeyValueHelper
{
    public static byte[] GetKey(int i)
    {
        return Encoding.UTF8.GetBytes($"aligo-keys {i}");
    }

    public static byte[] GetValue(int i)
    {
        return Encoding.UTF8.GetBytes($"aligo-values {i}");
    }
}