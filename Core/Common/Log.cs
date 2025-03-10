using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Core.Common;

public static class Log
{
    public static ILoggerFactory Factory { get; set; }

    static Log()
    {
        Factory = LoggerFactory.Create(b =>
        {
            b.Services.AddLogging();
        });
    }
}