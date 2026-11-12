namespace Downloader;

/// <summary>
/// Represents a measurable piece of data that can be batched.
/// </summary>
internal interface IMeasurableBatchData
{
    /// <summary>
    /// Gets the number used to distinguish multiple batches.
    /// </summary>
    long BatchNumber { get; }

    /// <summary>
    /// Gets the size of the data container.
    /// </summary>
    /// <remarks>
    /// If the data is stored in an array, the <see cref="Size"/> is the length of the array.
    /// Typically, the array length is greater than the <see cref="ContentSize"/>.
    /// </remarks>
    int Size { get; }

    /// <summary>
    /// Gets the size of the actual data content.
    /// </summary>
    int ContentSize { get; }
}