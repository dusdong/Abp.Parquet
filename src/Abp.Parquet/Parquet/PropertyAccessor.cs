using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace Abp.Parquet;

/// <summary>
/// 高性能属性访问器，使用表达式树编译属性访问代码，避免反射性能开销
/// </summary>
/// <typeparam name="T">要访问属性的对象类型</typeparam>
internal class PropertyAccessor<T>
{
    private readonly Dictionary<string, Func<T, object>> _getters = 
        new Dictionary<string, Func<T, object>>(StringComparer.OrdinalIgnoreCase);
        
    private readonly Dictionary<string, Action<T, object>> _setters = 
        new Dictionary<string, Action<T, object>>(StringComparer.OrdinalIgnoreCase);
        
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="properties">要预编译访问器的属性集合</param>
    public PropertyAccessor(IEnumerable<PropertyInfo> properties)
    {
        foreach (var prop in properties)
        {
            CompileGetter(prop);
            CompileSetter(prop);
        }
    }
        
    /// <summary>
    /// 预编译属性的Get访问器
    /// </summary>
    private void CompileGetter(PropertyInfo prop)
    {
        if (!prop.CanRead) return;
            
        try
        {
            // 创建表达式 (T obj) => (object)obj.Property
            var parameter = Expression.Parameter(typeof(T), "obj");
            var property = Expression.Property(parameter, prop);
            var convert = Expression.Convert(property, typeof(object));
            var lambda = Expression.Lambda<Func<T, object>>(convert, parameter);
                
            _getters[prop.Name] = lambda.Compile();
        }
        catch (Exception ex)
        {
            // 如果编译失败，记录警告
            Debug.WriteLine($"为属性 {prop.Name} 编译Getter失败: {ex.Message}");
        }
    }
        
    /// <summary>
    /// 预编译属性的Set访问器
    /// </summary>
    private void CompileSetter(PropertyInfo prop)
    {
        if (!prop.CanWrite) return;
            
        try
        {
            // 创建表达式 (T obj, object value) => obj.Property = (PropertyType)value
            var objParam = Expression.Parameter(typeof(T), "obj");
            var valueParam = Expression.Parameter(typeof(object), "value");
                
            var property = Expression.Property(objParam, prop);
            var convertedValue = Expression.Convert(valueParam, prop.PropertyType);
            var assign = Expression.Assign(property, convertedValue);
                
            var lambda = Expression.Lambda<Action<T, object>>(assign, objParam, valueParam);
                
            _setters[prop.Name] = lambda.Compile();
        }
        catch (Exception ex)
        {
            // 如果编译失败，记录警告
            Debug.WriteLine($"为属性 {prop.Name} 编译Setter失败: {ex.Message}");
        }
    }
        
    /// <summary>
    /// 获取属性值 - 使用预编译访问器
    /// </summary>
    public object GetValue(T obj, string propertyName)
    {
        if (obj == null) throw new ArgumentNullException(nameof(obj));
        if (string.IsNullOrEmpty(propertyName)) throw new ArgumentNullException(nameof(propertyName));
            
        if (_getters.TryGetValue(propertyName, out var getter))
        {
            return getter(obj);
        }
            
        throw new ArgumentException($"属性 {propertyName} 不存在或无法访问", nameof(propertyName));
    }
        
    /// <summary>
    /// 设置属性值 - 使用预编译访问器
    /// </summary>
    public void SetValue(T obj, string propertyName, object value)
    {
        if (obj == null) throw new ArgumentNullException(nameof(obj));
        if (string.IsNullOrEmpty(propertyName)) throw new ArgumentNullException(nameof(propertyName));
            
        if (_setters.TryGetValue(propertyName, out var setter))
        {
            setter(obj, value);
            return;
        }
            
        throw new ArgumentException($"属性 {propertyName} 不存在或无法写入", nameof(propertyName));
    }
        
    /// <summary>
    /// 检查是否存在指定属性的访问器
    /// </summary>
    public bool HasProperty(string propertyName)
    {
        return _getters.ContainsKey(propertyName);
    }
}
