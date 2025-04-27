namespace Core.Exceptions;

public class ErrorMessage
{
    public const string OpenFileError = "Open file error";
    public const string ReadFileError = "Read file error";
    public const string WriteFileError = "Write file error";
    public const string SyncError = "Sync error";
    
    public const string WriteDataFileError = "Write data file error";
    public const string ReadLogRecordError = "Read log record error";
    
    public const string CreateIOManagerError = "Create IO manager error";
    
    public const string CreateDatabaseDirectoryError = "Create database directory error";
    public const string LoadMergeDataFilesError = "Load merge data files error";
    public const string EngineInitError = "Engine initialization error";
    public const string AppendLogRecordError = "Append log record error";
    public const string PutIndexError = "Put index error";
    public const string EnginePutError  = "Engine Put error";
    
    
    
    // redis 
    public const string WrongOperationType = "Wrong operation type";
    
}