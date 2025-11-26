using System.Text;

namespace Abp.Parquet;

/// <summary>
/// Parquet异常基类，提供统一的异常处理和上下文信息
/// </summary>
public abstract class ParquetException : Exception
{
    /// <summary>
    /// 异常发生的操作名称
    /// </summary>
    public string Operation { get; }
        
    /// <summary>
    /// 资源路径（通常是文件路径）
    /// </summary>
    public string ResourcePath { get; }
        
    /// <summary>
    /// 附加上下文信息
    /// </summary>
    public IDictionary<string, object> Context { get; }
        
    /// <summary>
    /// 构造函数
    /// </summary>
    protected ParquetException(string message, string operation, string resourcePath, 
        IDictionary<string, object> context = null, Exception innerException = null)
        : base(message, innerException)
    {
        Operation = operation;
        ResourcePath = resourcePath;
        Context = context ?? new Dictionary<string, object>();
    }
        
    /// <summary>
    /// 格式化异常信息
    /// </summary>
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.AppendLine($"{GetType().Name}: {Message}");
            
        if (!string.IsNullOrEmpty(Operation))
            sb.AppendLine($"操作: {Operation}");
                
        if (!string.IsNullOrEmpty(ResourcePath))
            sb.AppendLine($"资源路径: {ResourcePath}");
                
        if (Context.Count > 0)
        {
            sb.AppendLine("上下文信息:");
            foreach (var kv in Context)
            {
                sb.AppendLine($"  {kv.Key}: {kv.Value}");
            }
        }
            
        if (StackTrace != null)
            sb.AppendLine(StackTrace);
                
        if (InnerException != null)
        {
            sb.AppendLine("内部异常:");
            sb.AppendLine(InnerException.ToString());
        }
            
        return sb.ToString();
    }
}

/// <summary>
/// Parquet 解析异常 - 读取或解析时发生错误
/// </summary>
public class ParquetParserException : ParquetException
{
    /// <summary>
    /// 构造函数
    /// </summary>
    public ParquetParserException(string message, string operation = "Parse", string filePath = null,
        IDictionary<string, object> context = null, Exception innerException = null)
        : base(message, operation, filePath, context, innerException)
    {
    }
}

/// <summary>
/// Parquet 文件写入异常
/// </summary>
public class ParquetWriterException : ParquetException
{
    /// <summary>
    /// 构造函数
    /// </summary>
    public ParquetWriterException(string message, string operation = "Write", string filePath = null,
        IDictionary<string, object> context = null, Exception innerException = null)
        : base(message, operation, filePath, context, innerException)
    {
    }
}

/// <summary>
/// Parquet 类型转换异常 - 处理数据类型转换过程中的错误
/// </summary>
public class ParquetTypeConversionException : ParquetException
{
    /// <summary>
    /// 源类型
    /// </summary>
    public Type SourceType => 
        Context.TryGetValue("SourceType", out var src) ? src as Type : null;
            
    /// <summary>
    /// 目标类型
    /// </summary>
    public Type TargetType => 
        Context.TryGetValue("TargetType", out var tgt) ? tgt as Type : null;
            
    /// <summary>
    /// 字段名称
    /// </summary>
    public string FieldName => 
        Context.TryGetValue("FieldName", out var fieldVal) ? fieldVal as string : null;

    /// <summary>
    /// 构造函数
    /// </summary>
    public ParquetTypeConversionException(string message, Type sourceType, Type targetType, string fieldName,
        string operation = "TypeConversion", string filePath = null, Exception innerException = null)
        : base(message, operation, filePath, 
            new Dictionary<string, object>
            {
                { "SourceType", sourceType },
                { "TargetType", targetType },
                { "FieldName", fieldName }
            }, 
            innerException)
    {
    }
}

/// <summary>
/// Parquet 操作取消异常 - 当操作被用户取消时使用
/// </summary>
public class ParquetOperationCanceledException : ParquetException
{
    /// <summary>
    /// 构造函数
    /// </summary>
    public ParquetOperationCanceledException(string message, string operation = "Unknown",
        string resourcePath = null, Exception innerException = null)
        : base(message, operation, resourcePath, null, innerException)
    {
    }
}
