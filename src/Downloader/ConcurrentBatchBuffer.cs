using Downloader.Exceptions;
using System.Collections;
using System.Runtime.CompilerServices;

namespace Downloader;

/// <summary>
/// Represents a batchable bounded buffer that concurrently supports multiple producers and single consumer.
/// </summary>
/// <typeparam name="T">Specifies the type of the elements in the buffer.</typeparam>
internal sealed class ConcurrentBatchBuffer<T>
    where T : notnull, IMeasurableBatchData
{
    private readonly
#if NET9_0_OR_GREATER
        Lock
#else
        object
#endif
        _lock = new();
    private Storage _storage = null;
    private readonly long _capacity;
    private readonly long _batchThreshold;
    private bool _readyForRead = false;
    private bool _full = false;
    private bool _closed = false;
    private TaskCompletionSource _readBlocker = null;
    private TaskCompletionSource _writeBlocker = null;

    public ConcurrentBatchBuffer(long batchThreshold, long capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchThreshold, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 0);
        if (batchThreshold > capacity)
        {
            throw new ArgumentException($"The {nameof(batchThreshold)} is greater than the {nameof(capacity)}.");
        }

        _batchThreshold = batchThreshold;
        _capacity = capacity;
    }

    public sealed class Storage : IEnumerable<T>
    {
        internal class BatchList
        {
            public long ContentSize = 0;
            public List<T> Items = new();

            public void Clear()
            {
                ContentSize = 0;
                Items.Clear();
            }
        }

        internal readonly SortedDictionary<long, BatchList> _batchDict = new();
        internal bool _empty = true;
        internal long _size = 0;
        internal long _maxBatchSize = 0;

        public Storage()
        {
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (var list in _batchDict.Values)
            {
                foreach (var item in list.Items)
                {
                    yield return item;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        internal void Add(T item, long size, long contentSize, long batchThreshold, long capacity, out bool readyForRead, out bool full)
        {
            if (!_batchDict.TryGetValue(item.BatchNumber, out var list))
            {
                list = new();
                _batchDict[item.BatchNumber] = list;
            }

            var oldSize = _size;
            try
            {
                checked
                {
                    _size += size;
                    list.ContentSize += contentSize;
                }
            }
            catch (Exception)
            {
                _size = oldSize;
                throw;
            }
            if (list.ContentSize > _maxBatchSize)
            {
                _maxBatchSize = list.ContentSize;
            }

            full = _size >= capacity;
            readyForRead = full || _maxBatchSize >= batchThreshold;

            list.Items.Add(item);
            _empty = false;
        }

        internal void Clear()
        {
            foreach (var list in _batchDict.Values)
            {
                list.Clear();
            }
            _size = 0;
            _maxBatchSize = 0;
            _empty = true;
        }
    }

    /// <summary>
    /// Gets the maximum size that the buffer allows for storage.
    /// </summary>
    public long Capacity => _capacity;

    /// <summary>
    /// Gets the minimum size allowed for batching data, used with notifying the consumer.
    /// </summary>
    public long BatchThreshold => _batchThreshold;

    public bool IsClosed => Volatile.Read(ref _closed);

    /// <summary>
    /// Asynchronously swaps the storage to consume the data.
    /// </summary>
    /// <param name="storage">The old storage.</param>
    /// <param name="cancellationToken">The cancellation token used to cancel the swap operation.</param>
    /// <returns>A task that represents the asynchronous swap operation. The task result is the new storage.</returns>
    /// <exception cref="OperationCanceledException">The <paramref name="cancellationToken"/> was canceled. This exception is stored into the returned task.</exception>
    /// <exception cref="BufferClosedException">The current buffer is empty and closed.</exception>
    /// <remarks>
    /// If the batch threshold is not reached, the swap operation will be blocked asynchronously.
    /// When the swap operation is actually performed, it will wake the producers that are blocked.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Storage> SwapAsync(Storage storage, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<Storage>(cancellationToken);
        }

        return SwapAsyncCore(storage, cancellationToken);
    }

    private async ValueTask<Storage> SwapAsyncCore(Storage storage, CancellationToken cancellationToken)
    {
        storage?.Clear();

        while (true)
        {
            Storage newStorage = null;
            TaskCompletionSource readBlocker = null, writeBlocker = null;

            lock (_lock)
            {
                if (_readyForRead)
                {
                    newStorage = _storage;
                    _storage = storage;

                    _readyForRead = false;
                    _full = false;

                    if (_writeBlocker is not null)
                    {
                        writeBlocker = _writeBlocker;
                        _writeBlocker = null;
                    }
                }
                else
                {
                    if (_closed)
                    {
                        throw new BufferClosedException();
                    }

                    _readBlocker ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
                    readBlocker = _readBlocker;
                }
            }

            if (readBlocker is not null)
            {
                await readBlocker.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            writeBlocker?.SetResult();

            return newStorage;
        }
    }

    /// <inheritdoc cref="WriteAsync(T, bool, CancellationToken)"/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WriteAsync(T item, CancellationToken cancellationToken = default)
        => WriteAsync(item, false, cancellationToken);

    /// <summary>
    /// Asynchronously writes an item to the buffer.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="forceReadable">Whether force the buffer to be readable.</param>
    /// <param name="cancellationToken">The cancellation token used to cancel the write operation.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    /// <exception cref="ArgumentException">
    /// The <see cref="IMeasurableBatchData.Size"/> or <see cref="IMeasurableBatchData.ContentSize"/> of the <paramref name="item"/> is negative,
    /// or the <see cref="IMeasurableBatchData.ContentSize"/> is greater than the <see cref="IMeasurableBatchData.Size"/>.
    /// </exception>
    /// <exception cref="ArgumentNullException">The <paramref name="item"/> is <c>null</c>.</exception>
    /// <exception cref="OperationCanceledException">The <paramref name="cancellationToken"/> was canceled. This exception is stored into the returned task.</exception>
    /// <exception cref="BufferClosedException">The current buffer is closed.</exception>
    /// <remarks>
    /// If the batch threshold is reached, it will make the storage readable and wake the consumer if the consumer is blocked.
    /// If the size limit is reached, the write operation will be blocked asynchronously.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WriteAsync(T item, bool forceReadable, CancellationToken cancellationToken = default)
    {
        if (item is null)
        {
            throw new ArgumentNullException(nameof(item));
        }
        var size = item.Size;
        if (size < 0)
        {
            throw new ArgumentException($"The {nameof(IMeasurableBatchData.Size)} of the item is negative", nameof(item));
        }
        var contentSize = item.ContentSize;
        if (contentSize < 0)
        {
            throw new ArgumentException($"The {nameof(IMeasurableBatchData.ContentSize)} of the item is negative", nameof(item));
        }
        if (contentSize > size)
        {
            throw new ArgumentException($"The {nameof(IMeasurableBatchData.ContentSize)} of the item is greater than the {nameof(IMeasurableBatchData.Size)}.");
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        return WriteAsyncCore(item, size, contentSize, forceReadable, cancellationToken);
    }

    private async Task WriteAsyncCore(T item, long size, long contentSize, bool forceReadable, CancellationToken cancellationToken)
    {
        while (true)
        {
            TaskCompletionSource writeBlocker = null, readBlocker = null;

            lock (_lock)
            {
                if (_closed)
                {
                    throw new BufferClosedException();
                }

                if (_full)
                {
                    _writeBlocker ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
                    writeBlocker = _writeBlocker;
                }
                else
                {
                    _storage ??= new();
                    _storage.Add(item, size, contentSize, _batchThreshold, _capacity, out _readyForRead, out _full);
                    if (forceReadable)
                    {
                        _readyForRead = true;
                    }

                    if (_readyForRead)
                    {
                        if (_readBlocker is not null)
                        {
                            readBlocker = _readBlocker;
                            _readBlocker = null;
                        }
                    }
                }
            }

            if (writeBlocker is not null)
            {
                await writeBlocker.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            readBlocker?.SetResult();

            return;
        }
    }

    /// <summary>
    /// Closes the buffer and wakes read or write operations that are blocked. Subsequent writes will fail.
    /// </summary>
    public void Close()
    {
        TaskCompletionSource readBlocker, writeBlocker;
        
        lock (_lock)
        {
            if (_closed)
            {
                return;
            }
            _closed = true;

            _readyForRead = !(_storage?._empty ?? true);

            readBlocker = _readBlocker;
            _readBlocker = null;

            writeBlocker = _writeBlocker;
            _writeBlocker = null;
        }

        readBlocker?.SetResult();
        writeBlocker?.SetResult();
    }
}