using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Abp.Parquet;

/// <summary>
/// 反射辅助类，提供缓存和优化功能
/// </summary>
public static class ReflectionHelper
{
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _propertyInfoCache = 
        new ConcurrentDictionary<Type, PropertyInfo[]>();
        
    private static readonly ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>> _propertyByNameCache = 
        new ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>>();
        
    private static readonly ConcurrentDictionary<PropertyInfo, Func<object, object>> _propertyGetterCache = 
        new ConcurrentDictionary<PropertyInfo, Func<object, object>>();
    
    private static readonly ConcurrentDictionary<PropertyInfo, Action<object, object>> _propertySetterCache = 
        new ConcurrentDictionary<PropertyInfo, Action<object, object>>();
    
    private static readonly ConcurrentDictionary<Type, Func<object>> _activatorCache = 
        new ConcurrentDictionary<Type, Func<object>>();
    
    /// <summary>
    /// 获取指定类型的所有公共实例属性
    /// </summary>
    /// <param name="type">类型</param>
    /// <returns>属性信息数组</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PropertyInfo[] GetProperties(Type type)
    {
        return _propertyInfoCache.GetOrAdd(type, t => 
            t.GetProperties(BindingFlags.Public | BindingFlags.Instance));
    }
    
    /// <summary>
    /// 获取指定类型的属性字典（按名称索引）
    /// </summary>
    /// <param name="type">类型</param>
    /// <returns>属性字典</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Dictionary<string, PropertyInfo> GetPropertyDictionary(Type type)
    {
        return _propertyByNameCache.GetOrAdd(type, t =>
        {
            var properties = GetProperties(t);
            return properties.ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);
        });
    }
    
    /// <summary>
    /// 获取指定属性的高性能getter
    /// </summary>
    /// <param name="property">属性</param>
    /// <returns>属性getter委托</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Func<object, object> GetPropertyGetter(PropertyInfo property)
    {
        return _propertyGetterCache.GetOrAdd(property, p =>
        {
            var getter = p.GetGetMethod();
            if (getter == null) return null;
            
            return obj => getter.Invoke(obj, null);
        });
    }
    
    /// <summary>
    /// 获取指定属性的高性能setter
    /// </summary>
    /// <param name="property">属性</param>
    /// <returns>属性setter委托</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Action<object, object> GetPropertySetter(PropertyInfo property)
    {
        return _propertySetterCache.GetOrAdd(property, p =>
        {
            var setter = p.GetSetMethod();
            if (setter == null) return null;
            
            return (obj, value) => setter.Invoke(obj, new[] { value });
        });
    }
    
    /// <summary>
    /// 获取指定类型的高性能实例创建器
    /// </summary>
    /// <param name="type">类型</param>
    /// <returns>实例创建委托</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Func<object> GetActivator(Type type)
    {
        return _activatorCache.GetOrAdd(type, t =>
        {
            var ctor = t.GetConstructor(Type.EmptyTypes);
            if (ctor == null) return null;
            
            return () => ctor.Invoke(null);
        });
    }
    
    /// <summary>
    /// 清除所有缓存
    /// </summary>
    public static void ClearAllCaches()
    {
        _propertyInfoCache.Clear();
        _propertyByNameCache.Clear();
        _propertyGetterCache.Clear();
        _propertySetterCache.Clear();
        _activatorCache.Clear();
    }
}
