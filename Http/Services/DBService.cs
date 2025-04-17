using System.Text;
using Core.DB;
using Core.IO;
using Microsoft.Extensions.Options;
namespace Http.Services;

public class DBService
{
    private readonly Engine _engine;
    public DBService(IOptions<DBOptions> options)
    {
        var v = options.Value;
        if (!Enum.TryParse<IOManagerType>(v.IOType, true, out var ioType))
        {
            //throw new ArgumentException("Invalid IO type");
            ioType = IOManagerType.FileIO;
        }

        var dir = Environment.CurrentDirectory;
        var option = new EngineOptions(Path.Join(dir,v.DirPath),v.DataFileSize,v.AlwaySync,ioType,v.ReclaimableRatio);
        _engine = new Engine(option);
    }


    public string Get(string key)
    {
        var result =_engine.Get(  Encoding.UTF8.GetBytes(key));
        return Encoding.UTF8.GetString(result);
    }

    public bool Put(string key, string value)
    {
        return _engine.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
    }

    public bool Delete(string key)
    {
        return _engine.Delete(Encoding.UTF8.GetBytes(key));
    }

    public bool Contains(string key)
    {
       return _engine.Contains(Encoding.UTF8.GetBytes(key));
    }

    public void Merge()
    {
        _engine.Merge();
    }

    public void Sync()
    {
        _engine.Sync();
    }

    public void Backup(string directory)
    {
        _engine.BackUp(directory);
    }

    public  IEnumerable<string> GetKeys()
    {
        return _engine.GetKeys()
            .Select(x => Encoding.UTF8.GetString(x));
    }

    public Stat Stat()
    {
        return _engine.GetStat();
    }
}