using Core.DB;
using Http.Services;
using Microsoft.AspNetCore.Mvc;

namespace Http.Controllers;

[ApiController]
[Route("Api/[controller]")]
[ProducesResponseType(StatusCodes.Status500InternalServerError)]
public class BitcastController: ControllerBase
{
    private readonly DBService _dbService;

    public BitcastController(DBService dbService)
    {
        _dbService = dbService;
    }

    [HttpGet("{key}")]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public ActionResult<string> Get(string key)
    {   
        // 异常处理有问题,没有考虑获取不到值的情况，值不存在，正常情况。
        // 以及其他异常意外情况
        try
        {
            return _dbService.Get(key);
        }
        catch (Exception e)
        {
          return BadRequest("Key not found");
        }
      
    }


    [HttpPost]
    [Consumes("application/json")]
    public ActionResult<bool> Put(KeyValuePair<string,string> value)
    {
        try
        {
            var result = _dbService.Put(value.Key, value.Value);
            return  CreatedAtAction(nameof(Put), new { key = value.Key, value.Value });
        }
        catch (Exception e)
        {
            return BadRequest("put failed");
        }
    }


    [HttpDelete("{key}")]
    public ActionResult<byte[]> Delete(string key)
    {
        try
        {
             return _dbService.Delete(key);
        }
        catch (Exception e)
        {
            return BadRequest("delete failed");
        }
    }


    [HttpGet("Stat")]
    public ActionResult<Stat> GetStat()
    {
        return _dbService.Stat();
    }

    [HttpPost("Keys")]
    public ActionResult<List<string>> Keys()
    {
        return _dbService.GetKeys().ToList();
    }
    
}