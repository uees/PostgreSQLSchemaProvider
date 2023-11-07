using System.Data.Common;

namespace SchemaExplorer
{
    public static class SqlDataReaderExtension
    {
        public static T GetValueOrDefault<T>(this DbDataReader reader, string name)
        {
            int ordinal = reader.GetOrdinal(name);
            if (!reader.IsDBNull(ordinal))
            {
                return reader.GetFieldValue<T>(ordinal);
            }
            return default(T);
        }
    }
}