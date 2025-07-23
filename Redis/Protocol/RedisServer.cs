using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Redis.Protocol;

public class RedisServer
{
    
    private bool _isRunning = true;
    public RedisServer(IPEndPoint endpoint)
    {
        this.EndPoint = endpoint;
    }
    
    public IPEndPoint EndPoint { get; set; }
    public event Action<RESPDataType,RESPWriter> Receives;

    public void Stop()
    {
        _isRunning = false;
    }
    public async Task Run()
    {
        try
        {
            using Socket server = new Socket(EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            while (_isRunning)
            {
                var client = await server.AcceptAsync();
                new Thread( () =>
                {
                    ClientHandler(client).Wait();
                }).Start();
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

   
    private async Task ClientHandler(Socket client)
    {
        try
        {
            var buffer = new byte[1024];
            while (true)
            {
                
                var count = await client.ReceiveAsync(buffer,SocketFlags.None);
                var (type, data) = new RESPparser().Parse(Encoding.ASCII.GetString(buffer, 0, count));
                // invoke handler 
                Receives?.Invoke(type,new RESPWriter(client));
            }
        }
        catch (Exception e)
        {
           Console.WriteLine(e);
        }
        finally
        {
            client.Shutdown(SocketShutdown.Both);
        }

    }
    
}