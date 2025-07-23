namespace Redis.Protocol;

public enum RESPDataType
{
    SimpleString,
    SimpleError,
    Integers,
    BulkStrings,
    Arrays,
    Nulls,
    Booleans,
    Doubles,
    BigNumbers,
    BulkErrors,
    VerbatimStrings,
    Maps,
    Attributes,
    Sets,
    Pushes,
}