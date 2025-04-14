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
        return new FileIO(path);
    }

    public static IIOManager CreateMemoryMap(string path)
    {
        return new MemoryMap(path);
    }
}