using System.Reflection;
using Parquet.Schema;

namespace Abp.Parquet;

/// <summary>
/// 负责根据记录类型和配置生成 ParquetSchema。
/// </summary>
/// <typeparam name="T">记录类型。</typeparam>
public class ParquetSchemaGenerator<T>
{
    private readonly ParquetWriterConfiguration _configuration;
    private readonly Type _recordType;
        
    // 字段名称与原始属性类型的映射
    private readonly Dictionary<string, Type> _fieldOriginalTypes = new Dictionary<string, Type>();

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="configuration">Parquet 写入器配置，如果为 null，则使用默认配置</param>
    public ParquetSchemaGenerator(ParquetWriterConfiguration configuration = null)
    {
        _recordType = typeof(T) ?? throw new ArgumentNullException(nameof(T));
        _configuration = configuration ?? new ParquetWriterConfiguration();
    }

    /// <summary>
    /// 根据当前配置和记录类型生成 ParquetSchema。
    /// </summary>
    /// <returns>生成的 ParquetSchema。</returns>
    public ParquetSchema GenerateSchema()
    {
        var fields = GetSchemaFields();
        return new ParquetSchema(fields.Values.ToArray());
    }

    /// <summary>
    /// 根据配置获取架构字段。
    /// </summary>
    /// <returns>字段名称与对应 DataField 的字典。</returns>
    private IDictionary<string, DataField> GetSchemaFields()
    {
        var fields = new Dictionary<string, DataField>();
            
        // 使用反射获取类型 T 的所有公共属性
        var properties = _recordType.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy);

        foreach (var prop in properties)
        {
            // 如果属性在忽略列表中，则跳过
            if (_configuration.IgnoredFields.Contains(prop.Name))
                continue;

            // 获取 Parquet 类型和可空性
            var parquetType = GetParquetType(prop.PropertyType);
            bool isNullable = IsNullableType(prop.PropertyType);

            // 创建 DataField 并添加到字典
            fields.Add(prop.Name, new DataField(prop.Name, parquetType, isNullable));
            _fieldOriginalTypes[prop.Name] = prop.PropertyType;
        }

        return fields;
    }

    /// <summary>
    /// 确定给定 .NET 类型对应的 Parquet 类型。
    /// </summary>
    /// <param name="type">.NET 类型。</param>
    /// <returns>对应的 Parquet 类型。</returns>
    private Type GetParquetType(Type type)
    {
        // 应用配置中的自定义类型映射
        if (_configuration.MapParquetType != null)
        {
            type = _configuration.MapParquetType(type);
        }

        // 处理可空类型，获取基础类型
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        // 处理特殊类型
        if (underlyingType == typeof(Guid))
            return typeof(string); // 将 Guid 转换为 string

        if (underlyingType == typeof(TimeSpan))
            return typeof(string); // 将 TimeSpan 转换为 string

        if (underlyingType == typeof(byte[]))
            return typeof(byte[]); // 直接使用 byte[]

        // 根据类型转换规则处理不同类型
        switch (Type.GetTypeCode(underlyingType))
        {
            case TypeCode.DateTime:
                if (_configuration.TreatDateTimeAsDateTimeOffset)
                    return typeof(DateTimeOffset);
                else if (_configuration.TreatDateTimeAsString)
                    return typeof(string);
                else
                    return typeof(DateTime);
                    
            case TypeCode.String:
                return typeof(string);
                    
            case TypeCode.Int32:
                return typeof(int);
                    
            case TypeCode.Int64:
                return typeof(long);
                    
            case TypeCode.Decimal:
                return typeof(decimal);
                    
            case TypeCode.Double:
                return typeof(double);
                    
            case TypeCode.Boolean:
                return typeof(bool);
                    
            case TypeCode.Byte:
                return typeof(byte);
                    
            default:
                if (underlyingType.IsEnum)
                    return typeof(string);
                        
                if (underlyingType == typeof(DateTimeOffset))
                    return _configuration.TreatDateTimeOffsetAsString ? typeof(string) : typeof(DateTimeOffset);

                // 对于其他类型，将其视为字符串
                return typeof(string);
        }
    }

    /// <summary>
    /// 判断类型是否为可空类型。
    /// </summary>
    /// <param name="type">要检查的类型</param>
    /// <returns>如果类型是可空的，则为 true，否则为 false</returns>
    private bool IsNullableType(Type type)
    {
        return Nullable.GetUnderlyingType(type) != null || !type.IsValueType;
    }

    /// <summary>
    /// 获取字段的原始属性类型
    /// </summary>
    /// <param name="fieldName">字段名称</param>
    /// <returns>原始属性类型</returns>
    public Type GetOriginalFieldType(string fieldName)
    {
        if (_fieldOriginalTypes.TryGetValue(fieldName, out Type originalType))
            return originalType;
                
        return typeof(string); // 默认返回字符串类型
    }
}
