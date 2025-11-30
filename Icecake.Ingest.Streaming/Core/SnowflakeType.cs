namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents the data types supported by Snowflake for column definitions and data conversions.
/// </summary>
public enum SnowflakeType
{
    NUMBER,
    BOOLEAN,
    VARCHAR,
    BINARY,
    VARIANT,
    DATE,
    TIME,
    TIMESTAMP_NTZ,
    TIMESTAMP_LTZ,
    TIMESTAMP_TZ
}