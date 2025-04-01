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
    /// 私有构造函数，由CreateAsync工厂方法调用
    /// </summary>
    private ParquetFileWriter(string filePath, ParquetSchema schema = null, ParquetWriterConfiguration configuration = null)
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
    /// 创建 ParquetFileWriter 的异步工厂方法
    /// </summary>
    /// <param name="filePath">目标 Parquet 文件路径</param>
    /// <param name="schema">可选的 Parquet 架构，如果为 null，则自动生成</param>
    /// <param name="configuration">可选的配置对象，如果为 null，则使用默认配置</param>
    /// <returns>初始化完成的 ParquetFileWriter 实例</returns>
    public static async Task<ParquetFileWriter<T>> CreateAsync(string filePath, ParquetSchema schema = null, ParquetWriterConfiguration configuration = null)
    {
        var writer = new ParquetFileWriter<T>(filePath, schema, configuration);
        await writer.InitializeAsync();
        return writer;
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

            try
            {
                // 打开 FileStream - 这是真正的异步IO操作
                _fileStream = new FileStream(
                    _filePath,
                    fileExists ? FileMode.Open : FileMode.Create,
                    FileAccess.ReadWrite,
                    FileShare.Read,
                    4096, // 默认缓冲区大小
                    FileOptions.Asynchronous); // 启用异步IO

                if (fileExists)
                {
                    // 如果是追加模式，确保流指针位于文件末尾
                    _fileStream.Seek(0, SeekOrigin.End);
                }

                // 创建ParquetWriter - 使用异步方法
                _parquetWriter = await ParquetWriter.CreateAsync(_schema, _fileStream, append: fileExists);

                _parquetWriter.CompressionMethod = _configuration.CompressionMethod;
                _parquetWriter.CompressionLevel = _configuration.CompressionLevel;

                if (_configuration.CustomMetadata != null)
                    _parquetWriter.CustomMetadata = _configuration.CustomMetadata;

                _isInitialized = true;
            }
            catch (Exception ex)
            {
                // 确保在初始化失败时释放资源
                _parquetWriter?.Dispose();
                _fileStream?.Dispose();
                throw new ParquetWriterException($"初始化Parquet写入器失败: {ex.Message}", ex);
            }
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
    /// 写入数据到 Parquet 文件 (同步版本)
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    public void Write(List<T> records)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null || !records.Any())
            return;

        // 确保初始化 - 同步等待异步初始化
        InitializeAsync().GetAwaiter().GetResult();

        try
        {
            // 注意：创建的是同步调用
            using var rowGroupWriter = _parquetWriter.CreateRowGroup();

            // 构建字段名称到 DataField 的字典
            var dataFieldDict = _schema.Fields
                .OfType<DataField>()
                .ToDictionary(f => f.Name, f => f);

            // 自动创建 DataColumn 列表
            var dataColumns = CreateDataColumns(records, dataFieldDict);

            // 写入所有列 - 由于WriteColumnAsync只有异步版本，所以在同步方法中同步等待
            foreach (var dataColumn in dataColumns)
            {
                // 使用同步等待异步操作
                rowGroupWriter.WriteColumnAsync(dataColumn).GetAwaiter().GetResult();
            }

            // 刷新流，确保数据写入
            _fileStream.Flush();
        }
        catch (Exception ex)
        {
            throw new ParquetWriterException($"写入Parquet数据失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 写入数据到 Parquet 文件 (异步版本)
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>完成任务</returns>
    public async Task WriteAsync(List<T> records, CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null || !records.Any())
            return;

        // 确保初始化
        await InitializeAsync().ConfigureAwait(false);

        try
        {
            // 创建行组 - 同步调用
            var rowGroupWriter = _parquetWriter.CreateRowGroup();
            using (rowGroupWriter)
            {
                // 构建字段名称到 DataField 的字典
                var dataFieldDict = _schema.Fields
                    .OfType<DataField>()
                    .ToDictionary(f => f.Name, f => f);

                // 自动创建 DataColumn 列表 - 这是CPU密集型的，可以考虑在后台线程执行
                var dataColumns = await Task.Run(() => CreateDataColumns(records, dataFieldDict), cancellationToken);

                // 写入所有列 - 使用正确的异步方法
                foreach (var dataColumn in dataColumns)
                {
                    await rowGroupWriter.WriteColumnAsync(dataColumn);
                }
            }

            // 异步刷新流
            await _fileStream.FlushAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw; // 让取消异常直接传播
        }
        catch (Exception ex)
        {
            throw new ParquetWriterException($"异步写入Parquet数据失败: {ex.Message}", ex);
        }
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
                        fieldValues = rawValues.Select(val => val != null ? val.ToString() : null).ToArray();
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
                        fieldValues = rawValues.Select(val => val != null ? val.ToString() : null).ToArray();
                    }
                }

                // 创建 DataColumn 并添加到列表
                var column = new DataColumn(field, fieldValues);
                dataColumns.Add(column);
            }
            catch (Exception ex)
            {
                throw new ParquetWriterException($"写入字段 '{fieldName}' 时发生错误: {ex.Message}", ex);
            }
        }

        return dataColumns;
    }

    /// <summary>
    /// 获取指定类型的默认值。
    /// </summary>
    private static object GetDefault(Type type)
    {
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_isDisposed)
            return;

        _parquetWriter?.Dispose();
        _fileStream?.Dispose();
        _initSemaphore?.Dispose();
        _isDisposed = true;

        GC.SuppressFinalize(this);
    }
}
