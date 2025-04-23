namespace Redis;

public enum Types
{
    String,
    Hash,
    Set,
    List,
    ZSet,
}


public class TypesHelper
{
    public static Types GetType(int type)
    {
        return type switch
        {
            0 => Types.String,
            1 => Types.Hash,
            2 => Types.Set,
            3 => Types.List,
            4=> Types.ZSet,
            _ => throw new NotSupportedException(),
        };
    }
}
