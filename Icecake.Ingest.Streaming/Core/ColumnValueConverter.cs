using System.Text.Json;
using Icecake.Ingest.Streaming.Shims;

namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Provides functionality to convert input values into formats compatible with Snowflake data types.
/// </summary>
public static class ColumnValueConverter
{
    /// <summary>
    /// Converts an input value to a format compatible with the specified Snowflake column type.
    /// </summary>
    /// <param name="spec">
    /// The column specification, which includes the Snowflake data type and optional precision.
    /// </param>
    /// <param name="value">
    /// The input value to be converted. This value may be null.
    /// </param>
    /// <returns>
    /// The input value converted to the corresponding Snowflake data type format, or null if the input value is null.
    /// If the conversion fails, an exception may be thrown.
    /// </returns>
    public static object? ConvertInternal(ColumnSpec spec, object? value)
    {
        if (value is null) return null;
        return spec.Type switch
        {
            SnowflakeType.BOOLEAN => ToBoolean(value),
            SnowflakeType.NUMBER => ToNumber(spec, value),
            SnowflakeType.VARCHAR => ToVarchar(spec, value),
            SnowflakeType.BINARY => ToBinaryBase64(value),
            SnowflakeType.VARIANT => ToVariant(value),
            SnowflakeType.DATE => ToDateString(value),
            SnowflakeType.TIME => ToTimeString(spec, value),
            SnowflakeType.TIMESTAMP_NTZ or SnowflakeType.TIMESTAMP_LTZ or SnowflakeType.TIMESTAMP_TZ => ToTimestampIso(spec, value),
            _ => value
        };
    }

    private static object ToBoolean(object v) => v switch
    {
        bool b => b, string s when bool.TryParse(s, out var b) => b,
        sbyte or byte or short or ushort or int or uint or long or ulong => Convert.ToInt64(v) != 0,
        _ => throw new ArgumentException($"Cannot coerce to BOOLEAN: {v.GetType()}")
    };

    private static object ToNumber(ColumnSpec spec, object v)
    {
        var d = v switch
        {
            decimal m => m, double db => (decimal)db, float f => (decimal)f,
            sbyte or byte or short or ushort or int or uint or long or ulong => Convert.ToDecimal(v),
            string s when decimal.TryParse(s, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var m) => m,
            _ => throw new ArgumentException($"Cannot coerce to NUMBER: {v.GetType()}")
        };
        if (spec.Scale is { } scale) d = Math.Round(d, scale, MidpointRounding.AwayFromZero);
        if (spec.Precision is { } p)
        {
            var abs = Math.Abs(d);
            var digits = abs == 0 ? 1 : (int)Math.Floor(Math.Log10((double)abs)) + 1;
            if (digits > p) throw new ArgumentOutOfRangeException(spec.Name, $"NUMBER exceeds precision {p}");
        }

        return d;
    }

    private static object ToVarchar(ColumnSpec spec, object v)
    {
        var s = v switch
        {
            string s1 => s1, DateTime dt => dt.ToString("O"), DateTimeOffset dto => dto.ToString("O"), Guid g => g.ToString(),
            _ => v.ToString() ?? string.Empty
        };
#if NET6_0_OR_GREATER
        if (v is DateOnly d) s = d.ToString("yyyy-MM-dd");
        if (v is TimeOnly t) s = t.ToString("HH:mm:ss.fffffff");
#endif
        if (spec.Length is { } len && s.Length > len) throw new ArgumentOutOfRangeException(spec.Name, $"VARCHAR exceeds length {len}");
        return s;
    }

    private static object ToBinaryBase64(object v)
    {
        var bytes = v switch
        {
            byte[] b => b, ReadOnlyMemory<byte> rom => rom.ToArray(), string hex when IsHex(hex) => HexHelper.FromHexString(NormalizeHex(hex)),
            _ => throw new ArgumentException($"Cannot coerce to BINARY: {v.GetType()}")
        };
        return Convert.ToBase64String(bytes);
    }

    private static bool IsHex(string s)
    {
        s = s.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? s.Substring(2) : s;
        if (s.Any(t => !Uri.IsHexDigit(t)))
        {
            return false;
        }

        return s.Length % 2 == 0;
    }

    private static string NormalizeHex(string s) => s.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? s.Substring(2) : s;

    private static object ToVariant(object v)
    {
        if (v is string json)
        {
            try
            {
                return JsonDocument.Parse(json).RootElement.Clone();
            }
            catch
            {
                throw new ArgumentException("Invalid JSON for VARIANT");
            }
        }

        if (v is IReadOnlyDictionary<string, object?> map)
        {
            var dict = new Dictionary<string, object?>();
            foreach (var kv in map) dict[kv.Key] = ToVariantValue(kv.Value);
            return dict;
        }

        if (v is IEnumerable<object?> seq)
        {
            var list = new List<object?>();
            foreach (var e in seq) list.Add(ToVariantValue(e));
            return list;
        }

        return v;
    }

    private static object? ToVariantValue(object? v)
    {
        if (v is null) return null;
        return v switch
        {
            string s => s, bool b => b, decimal or double or float or int or long or short or byte => v, DateTime dt => dt.ToUniversalTime().ToString("O"),
            DateTimeOffset dto => dto.ToUniversalTime().ToString("O"), byte[] bytes => Convert.ToBase64String(bytes),
            IReadOnlyDictionary<string, object?> map => ToVariant(map), IEnumerable<object?> seq => ToVariant(seq), _ => v.ToString()
        };
    }

    private static object ToDateString(object v)
    {
#if NET6_0_OR_GREATER
        if (v is DateOnly d) return d.ToString("yyyy-MM-dd");
#endif
        if (v is DateTime dt) return dt.Date.ToString("yyyy-MM-dd");
        if (v is DateTimeOffset dto) return dto.Date.ToString("yyyy-MM-dd");
        if (v is string s && DateTime.TryParse(s, out var parsed)) return parsed.Date.ToString("yyyy-MM-dd");
        throw new ArgumentException("Cannot coerce to DATE");
    }

    private static object ToTimeString(ColumnSpec spec, object v)
    {
#if NET6_0_OR_GREATER
        if (v is TimeOnly t) return t.ToString(FormatForScale(spec.Scale));
#endif
        if (v is TimeSpan ts) return new DateTime(ts.Ticks, DateTimeKind.Utc).ToString(FormatForScale(spec.Scale));
        if (v is string s && TimeSpan.TryParse(s, out var parsed)) return new DateTime(parsed.Ticks, DateTimeKind.Utc).ToString(FormatForScale(spec.Scale));
        throw new ArgumentException("Cannot coerce to TIME");
    }

    private static object ToTimestampIso(ColumnSpec spec, object v)
    {
        var dto = v switch
        {
            DateTimeOffset d => d,
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, dt.Kind == DateTimeKind.Unspecified ? DateTimeKind.Utc : dt.Kind)),
            string s when DateTimeOffset.TryParse(s, out var parsed) => parsed,
            long epoch when spec.Scale is > 0 => DateTimeOffset.FromUnixTimeMilliseconds(epoch),
            _ => throw new ArgumentException("Cannot coerce to TIMESTAMP")
        };
        return dto.ToUniversalTime().ToString(FormatForScale(spec.Scale));
    }

    private static string FormatForScale(int? scale)
    {
        var s = MathCompat.Clamp(scale ?? 6, 0, 9);
        return s switch
        {
            0 => "yyyy-MM-dd'T'HH:mm:ss'Z'", 1 => "yyyy-MM-dd'T'HH:mm:ss.f'Z'", 2 => "yyyy-MM-dd'T'HH:mm:ss.ff'Z'", 3 => "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
            4 => "yyyy-MM-dd'T'HH:mm:ss.ffff'Z'", 5 => "yyyy-MM-dd'T'HH:mm:ss.fffff'Z'", 6 => "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'",
            7 => "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", 8 => "yyyy-MM-dd'T'HH:mm:ss.ffffffff'Z'", _ => "yyyy-MM-dd'T'HH:mm:ss.fffffffff'Z'"
        };
    }
}