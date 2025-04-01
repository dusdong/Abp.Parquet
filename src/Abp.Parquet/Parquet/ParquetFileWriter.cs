using System.Reflection;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Abp.Parquet;

/// <summary>
/// 负责将数据写入 Parquet 文件，支持自动生成架构和追加写入。
/// </summary>
/// <typeparam name="T">记录类型。</typeparam>
public class ParquetFileWriter<T> : IDisposable
{
    private readonly string _filePath;
    private readonly ParquetSchema _schema;
    private readonly ParquetWriterConfiguration _configuration;
    private readonly PropertyInfo[] _properties;
    private readonly ParquetSchemaGenerator<T> _schemaGenerator;

    // 字段名称与原始属性类型的映射
    private readonly IDictionary<string, Type> _fieldOriginalTypes;

    // 持久化的 FileStream 和 ParquetWriter
    private FileStream _fileStream;
    private ParquetWriter _parquetWriter;
    private bool _isDisposed = false;

    // 用于懒初始化的信号量
    private readonly SemaphoreSlim _initSemaphore = new SemaphoreSlim(1, 1);
    private bool _isInitialized = false;

    /// <summary>
    /// 构造函数，初始化文件路径、架构和配置。
    /// </summary>
    /// <param name="filePath">目标 Parquet 文件路径。</param>
    /// <param name="schema">可选的 Parquet 架构，如果为 null，则自动生成。</param>
    /// <param name="configuration">可选的配置对象，如果为 null，则使用默认配置。</param>
    public ParquetFileWriter(string filePath, ParquetSchema schema = null, ParquetWriterConfiguration configuration = null)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("文件路径不能为空。", nameof(filePath));

        _filePath = filePath;
        _configuration = configuration ?? new ParquetWriterConfiguration();

        if (schema != null)
        {
            _schema = schema;
            _fieldOriginalTypes = ExtractFieldTypesFromSchema(_schema);
        }
        else
        {
            _schemaGenerator = new ParquetSchemaGenerator<T>(_configuration);
            _schema = _schemaGenerator.GenerateSchema();
            _fieldOriginalTypes = new Dictionary<string, Type>();

            // 获取字段的原始属性类型
            foreach (var field in _schema.Fields.OfType<DataField>())
            {
                string fieldName = field.Name;
                Type originalType = _schemaGenerator.GetOriginalFieldType(fieldName);
                _fieldOriginalTypes[fieldName] = originalType;
            }
        }

        // 使用反射获取类型 T 的所有公共属性
        _properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
    }

    /// <summary>
    /// 异步初始化 FileStream 和 ParquetWriter。
    /// </summary>
    private async Task InitializeAsync()
    {
        if (_isInitialized)
            return;

        await _initSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_isInitialized)
                return;

            var fileExists = File.Exists(_filePath);

            // 打开 FileStream
            _fileStream = new FileStream(_filePath, fileExists ? FileMode.Open : FileMode.Create, FileAccess.ReadWrite, FileShare.Read);

            if (fileExists)
            {
                // 如果是追加模式，确保流指针位于文件末尾
                _fileStream.Seek(0, SeekOrigin.End);
            }

            // 创建 ParquetWriter
            _parquetWriter = await ParquetWriter.CreateAsync(
                _schema,
                _fileStream,
                _configuration.ParquetOptions,
                append: fileExists).ConfigureAwait(false);

            _parquetWriter.CompressionMethod = _configuration.CompressionMethod;
            _parquetWriter.CompressionLevel = _configuration.CompressionLevel;

            if (_configuration.CustomMetadata != null)
                _parquetWriter.CustomMetadata = _configuration.CustomMetadata;

            _isInitialized = true;
        }
        finally
        {
            _initSemaphore.Release();
        }
    }

    /// <summary>
    /// 从 ParquetSchema 中提取字段原始类型
    /// </summary>
    private static IDictionary<string, Type> ExtractFieldTypesFromSchema(ParquetSchema schema)
    {
        var fieldTypes = new Dictionary<string, Type>();
        foreach (var field in schema.Fields.OfType<DataField>())
        {
            // 如果是 Nullable<>, 获取基础类型；否则直接使用 ClrType
            Type baseType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
            fieldTypes[field.Name] = baseType;
        }

        return fieldTypes;
    }

    /// <summary>
    /// 异步写入数据到 Parquet 文件。
    /// </summary>
    /// <param name="records">要写入的数据列表。</param>
    /// <returns>任务。</returns>
    public async Task WriteAsync(List<T> records)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null || !records.Any())
            return;

        // 确保初始化
        await InitializeAsync().ConfigureAwait(false);

        using var rowGroupWriter = _parquetWriter.CreateRowGroup();

        // 构建字段名称到 DataField 的字典
        var dataFieldDict = _schema.Fields
            .OfType<DataField>()
            .ToDictionary(f => f.Name, f => f);

        // 自动创建 DataColumn 列表
        var dataColumns = CreateDataColumns(records, dataFieldDict);

        // 写入所有列
        foreach (var dataColumn in dataColumns)
        {
            await rowGroupWriter.WriteColumnAsync(dataColumn).ConfigureAwait(false);
        }

        // 刷新流，确保数据写入
        await _fileStream.FlushAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// 自动创建 DataColumn 列表。
    /// </summary>
    /// <param name="records">要写入的数据列表。</param>
    /// <param name="dataFieldDict">字段名称到 DataField 的字典。</param>
    /// <returns>DataColumn 列表。</returns>
    private List<DataColumn> CreateDataColumns(List<T> records, Dictionary<string, DataField> dataFieldDict)
    {
        var dataColumns = new List<DataColumn>();

        foreach (var field in _schema.Fields.OfType<DataField>())
        {
            string fieldName = field.Name;

            // 获取原始属性类型
            if (!_fieldOriginalTypes.TryGetValue(fieldName, out Type originalPropertyType))
            {
                Console.WriteLine($"警告: 原始类型未找到 for field '{fieldName}'。");
                continue;
            }

            // 使用反射获取对应属性
            var property = _properties.FirstOrDefault(p => p.Name.Equals(fieldName, StringComparison.OrdinalIgnoreCase));
            if (property == null)
            {
                // 属性不存在，记录警告并忽略该字段
                Console.WriteLine($"警告: 属性 '{fieldName}' 在类型 '{typeof(T).Name}' 中未找到，字段将被忽略。");
                continue;
            }

            // 提取字段值
            var rawValues = records.Select(r => property.GetValue(r, null)).ToArray();
            Array fieldValues;

            try
            {
                if (field.IsNullable)
                {
                    if (originalPropertyType.IsValueType && Nullable.GetUnderlyingType(originalPropertyType) != null)
                    {
                        // 处理可空值类型（如 int?）
                        Type baseType = Nullable.GetUnderlyingType(originalPropertyType);
                        if (baseType == null)
                        {
                            throw new ParquetWriterException($"无法获取字段 '{fieldName}' 的基础类型。");
                        }

                        // 创建 Nullable<T> 数组
                        var typedArray = Array.CreateInstance(originalPropertyType, rawValues.Length);
                        for (int i = 0; i < rawValues.Length; i++)
                        {
                            if (rawValues[i] != null)
                            {
                                typedArray.SetValue(Convert.ChangeType(rawValues[i], baseType), i);
                            }
                            else
                            {
                                typedArray.SetValue(null, i);
                            }
                        }

                        fieldValues = typedArray;
                    }
                    else if (!originalPropertyType.IsValueType)
                    {
                        // 处理引用类型（如 string）
                        // 确保生成 string[] 而不是 object[]
                        fieldValues = rawValues.Select(val => val != null ? val.ToString() : null).ToArray<string>();
                    }
                    else
                    {
                        throw new ParquetWriterException($"字段 '{fieldName}' 的类型无法处理。");
                    }
                }
                else
                {
                    // 非可空类型，直接转换
                    if (field.ClrType.IsValueType)
                    {
                        // 创建具体类型的数组
                        Type targetType = field.ClrType;
                        var typedArray = Array.CreateInstance(targetType, rawValues.Length);
                        for (int i = 0; i < rawValues.Length; i++)
                        {
                            if (rawValues[i] != null)
                            {
                                typedArray.SetValue(Convert.ChangeType(rawValues[i], targetType), i);
                            }
                            else
                            {
                                // 使用默认值避免 null
                                typedArray.SetValue(GetDefault(targetType), i);
                            }
                        }

                        fieldValues = typedArray;
                    }
                    else
                    {
                        // 处理引用类型（如 string）
                        fieldValues = rawValues.Select(val => val != null ? val.ToString() : null).ToArray<string>();
                    }
                }

                // 创建 DataColumn 并添加到列表
                var column = new DataColumn(field, fieldValues);
                dataColumns.Add(column);
            }
            catch (Exception ex)
            {
                throw new ParquetWriterException($"写入字段 '{fieldName}' 时发生错误。", ex);
            }
        }

        return dataColumns;
    }

    /// <summary>
    /// 获取指定类型的默认值。
    /// </summary>
    private object GetDefault(Type type)
    {
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    /// <summary>
    /// 释放资源。
    /// </summary>
    public void Dispose()
    {
        if (_isDisposed)
            return;

        _parquetWriter?.Dispose();
        _fileStream?.Dispose();
        _initSemaphore?.Dispose();

        _isDisposed = true;
    }
}
