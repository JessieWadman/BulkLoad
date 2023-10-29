using BulkLoad.EntityFrameworkCore.Abstractions.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Helpers;

internal static class EntityMetadataExtensions
{
    public static EntityMetadata GetEntityMetadata<T>(this DbContext dbContext)
    {
        var entityType = dbContext.Model.FindEntityType(typeof(T))
                         ?? throw new InvalidOperationException($"Type '{typeof(T).Name}' could not be found in db context!");

        var tableName = entityType.GetTableName();
        if (tableName == null)
            throw new InvalidOperationException($"The type '{typeof(T).Name} is not mapped to a table!");

        var primaryKey = entityType.FindPrimaryKey()?.Properties.Select(p => p.GetColumnName()).ToArray();
        if (primaryKey == null)
            throw new InvalidOperationException($"There is not primary key for type '{typeof(T).Name}'");

        EntityMetadata entityMetadata = new(
            entityType.GetProperties().Select(x => new EntityProperty(
                x.GetColumnName(),
                x.ValueGenerated is ValueGenerated.OnAddOrUpdate or ValueGenerated.OnAdd,
                GetDbType(x),
                x.IsNullable,
                x.Name
            )).ToList(),
            primaryKey,
            typeof(T),
            tableName,
            entityType.GetSchema());

        return entityMetadata;
    }

    private static string GetDbType(IProperty property)
    {
        var type = property.GetColumnType();

        if (property.IsNullable)
        {
            type = $"{type} NULL";
        }

        return type;
    }
}