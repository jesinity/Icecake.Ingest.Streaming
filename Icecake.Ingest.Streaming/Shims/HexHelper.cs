namespace Icecake.Ingest.Streaming.Shims;

/// <summary>
/// Provides helper methods for hexadecimal string manipulations and conversions.
/// </summary>
internal static class HexHelper
{
    /// <summary>
    /// Converts a hexadecimal string to a byte array.
    /// </summary>
    /// <param name="hex">A string containing hexadecimal digits. Must have an even length.</param>
    /// <returns>A byte array representing the input hexadecimal string.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the input string is null.</exception>
    /// <exception cref="FormatException">Thrown when the input string is not a valid hexadecimal string or has an odd length.</exception>
    public static byte[] FromHexString(string hex)
    {
        if (hex is null)
            throw new ArgumentNullException(nameof(hex));

        if (hex.Length % 2 != 0)
            throw new FormatException("Hex string must have even length.");

        var length = hex.Length / 2;
        var result = new byte[length];

        for (var i = 0; i < length; i++)
        {
            var high = ParseNibble(hex[2 * i]) << 4;
            var low  = ParseNibble(hex[2 * i + 1]);
            result[i] = (byte)(high | low);
        }

        return result;
    }

    private static int ParseNibble(char c)
    {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        throw new FormatException($"Invalid hex character '{c}'.");
    }
}