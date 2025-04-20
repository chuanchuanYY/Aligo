using Core.Exceptions;

namespace Core.IO;

public enum IOManagerType
{
    FileIO,
    MemoryMap,
}
internal class IOManagerFactory
{
    public static IIOManager Create(IOManagerType type, string path)
        => type switch
        {
            IOManagerType.FileIO => CreateFileIO(path),
            IOManagerType.MemoryMap => CreateMemoryMap(path),
            _=> throw new NotImplementedException()
        };
    public static IIOManager CreateFileIO(string path)
    {
        try
        {
            return new FileIO(path);
        }
        catch (Exception e)
        {
            throw new Exception(ErrorMessage.CreateIOManagerError,e);
        }
    }

    public static IIOManager CreateMemoryMap(string path)
    {

        try
        {
            return new MemoryMap(path);
        }
        catch (Exception e)
        {
            throw new Exception(ErrorMessage.CreateIOManagerError,e);
        }
     
    }
}