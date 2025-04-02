namespace Abp.Parquet;

/// <summary>
/// Parquet 类型转换注册表，用于支持不同类型之间的透明转换
/// </summary>
public class ParquetTypeConverterRegistry
{
    private static readonly Lazy<ParquetTypeConverterRegistry> _instance = 
        new Lazy<ParquetTypeConverterRegistry>(() => new ParquetTypeConverterRegistry());
    
    /// <summary>
    /// 全局实例
    /// </summary>
    public static ParquetTypeConverterRegistry Instance => _instance.Value;
    
    // 类型转换器字典
    private readonly Dictionary<(Type sourceType, Type targetType), Func<Array, Array>> _converters = new();
    
    /// <summary>
    /// 构造函数，注册默认转换器
    /// </summary>
    public ParquetTypeConverterRegistry()
    {
        // 注册默认转换器
        RegisterDefaultConverters();
    }
    
    /// <summary>
    /// 注册类型转换器
    /// </summary>
    /// <param name="sourceType">源类型</param>
    /// <param name="targetType">目标类型</param>
    /// <param name="converter">转换函数</param>
    public void Register(Type sourceType, Type targetType, Func<Array, Array> converter)
    {
        _converters[(sourceType, targetType)] = converter;
    }
    
    /// <summary>
    /// 尝试转换数组类型
    /// </summary>
    /// <param name="sourceArray">源数组</param>
    /// <param name="targetType">目标数组元素类型</param>
    /// <param name="result">转换结果</param>
    /// <returns>是否成功转换</returns>
    public bool TryConvert(Array sourceArray, Type targetType, out Array result)
    {
        if (sourceArray == null)
        {
            result = null;
            return false;
        }
        
        // 获取源数组的元素类型
        Type sourceType = sourceArray.GetType().GetElementType();
        if (sourceType == targetType)
        {
            result = sourceArray;
            return true;
        }
        
        // 尝试查找转换器
        if (_converters.TryGetValue((sourceType, targetType), out var converter))
        {
            result = converter(sourceArray);
            return true;
        }
        
        result = null;
        return false;
    }
    
    /// <summary>
    /// 注册默认转换器
    /// </summary>
    private void RegisterDefaultConverters()
    {
        // DateTime[] -> DateTime?[] 转换器
        Register(typeof(DateTime), typeof(DateTime?), array => {
            var source = (DateTime[])array;
            var result = new DateTime?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // int[] -> int?[] 转换器
        Register(typeof(int), typeof(int?), array => {
            var source = (int[])array;
            var result = new int?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // long[] -> long?[] 转换器
        Register(typeof(long), typeof(long?), array => {
            var source = (long[])array;
            var result = new long?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // double[] -> double?[] 转换器
        Register(typeof(double), typeof(double?), array => {
            var source = (double[])array;
            var result = new double?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // bool[] -> bool?[] 转换器
        Register(typeof(bool), typeof(bool?), array => {
            var source = (bool[])array;
            var result = new bool?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // decimal[] -> decimal?[] 转换器
        Register(typeof(decimal), typeof(decimal?), array => {
            var source = (decimal[])array;
            var result = new decimal?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // float[] -> float?[] 转换器
        Register(typeof(float), typeof(float?), array => {
            var source = (float[])array;
            var result = new float?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // byte[] -> byte?[] 转换器
        Register(typeof(byte), typeof(byte?), array => {
            var source = (byte[])array;
            var result = new byte?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // short[] -> short?[] 转换器
        Register(typeof(short), typeof(short?), array => {
            var source = (short[])array;
            var result = new short?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // Guid[] -> Guid?[] 转换器
        Register(typeof(Guid), typeof(Guid?), array => {
            var source = (Guid[])array;
            var result = new Guid?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // DateTimeOffset[] -> DateTimeOffset?[] 转换器
        Register(typeof(DateTimeOffset), typeof(DateTimeOffset?), array => {
            var source = (DateTimeOffset[])array;
            var result = new DateTimeOffset?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
        
        // TimeSpan[] -> TimeSpan?[] 转换器
        Register(typeof(TimeSpan), typeof(TimeSpan?), array => {
            var source = (TimeSpan[])array;
            var result = new TimeSpan?[source.Length];
            
            for (int i = 0; i < source.Length; i++)
            {
                result[i] = source[i];
            }
            
            return result;
        });
    }
    
    /// <summary>
    /// 添加自定义类型转换器
    /// </summary>
    /// <typeparam name="TSource">源类型</typeparam>
    /// <typeparam name="TTarget">目标类型</typeparam>
    /// <param name="converter">转换函数</param>
    public void AddConverter<TSource, TTarget>(Func<TSource[], TTarget[]> converter)
    {
        Register(typeof(TSource), typeof(TTarget), array => converter((TSource[])array));
    }
}
