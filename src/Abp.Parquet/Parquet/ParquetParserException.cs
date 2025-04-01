namespace Abp.Parquet;

/// <summary>
/// Parquet 解析异常
/// </summary>
public class ParquetParserException : Exception
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    public ParquetParserException(string message) : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetParserException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
