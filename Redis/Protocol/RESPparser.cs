using System.Text;

namespace Redis.Protocol;

public class RESPparser
{
    public (RESPDataType,object) Parse(string line)
    {
        if (line.Length < 1)
        {
            throw new ArgumentException("Invalid RESP data");
        }
        
        var typeChar = line[0];
        var type = GetType(typeChar);
        int pos = 1;
        while (line[pos] != '\r' || line[pos] != '\n')
        {
            if (pos >= line.Length)
            {
                throw new ArgumentException("Invalid RESP data");
            }
            pos += 1;
        }
        var data = line.Substring(1, pos);
        
        return (type, typeChar);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="c"></param>
    /// <returns></returns>
    /// <exception cref="FormatException"></exception>
    private RESPDataType GetType(char c)
    {
        return c switch
        {
            '+' => RESPDataType.SimpleString,
            '-' => RESPDataType.SimpleError,
            ':' => RESPDataType.Integers,
            '$' => RESPDataType.BulkStrings,
            '*' => RESPDataType.Arrays,
            '_' => RESPDataType.Nulls,
            '#' => RESPDataType.Booleans,
            ',' => RESPDataType.Doubles,
            '(' => RESPDataType.BigNumbers,
            '!' => RESPDataType.BulkErrors,
            '=' => RESPDataType.VerbatimStrings,
            '%' => RESPDataType.Maps,
            '`' => RESPDataType.Attributes,
            '~' => RESPDataType.Sets,
            '>' => RESPDataType.Pushes,
            _ => throw new FormatException("Unrecognized RESP data type")
        };
    }
}