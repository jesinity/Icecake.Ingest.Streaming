namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents the specification of a database column, including its name, type, and optional constraints such as precision, scale, or length.
/// </summary>
public sealed class ColumnSpec
{
    public required string Name { get; init; }
    public required SnowflakeType Type { get; init; }
    public int? Precision { get; init; }
    public int? Scale { get; init; }
    public int? Length { get; init; }
}