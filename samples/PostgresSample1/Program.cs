using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using SqlServerSample1;

var dbOptions = new DbContextOptionsBuilder<MyDbContext>()
    .LogTo(a => Debug.WriteLine(a))
    // .UseSqlServer("Server=(localdb)\\MSSQLLocalDB;Initial Catalog=bulk_insert_tests;Integrated Security=SSPI")
    .UseNpgsql("server=localhost;port=5432;database=postgres;uid=postgres;password=root")
    .Options;

await using var db = new MyDbContext(dbOptions);
db.Database.EnsureCreated();

var employees = new List<Employee>();

for (var i = 0; i < 8; i++)
{
    // Change this back and forth between two numbers 1-10 and observe output, to see
    // how records are soft deleted, and then recovered
    if (i == 3)
        continue;
    
    var name = "Person #" + i;
    
    employees.Add(new Employee
    {
        Id = i,
        Name = name,
        Deleted = null,
        Version = 1,
        IsAwesome = false,
        LastUpdatedUtc = new DateTime(2004, 01, 02)
    });
}
var source = employees.AsAsyncEnumerable();

var changes = db
    .PostgresLoadAndMerge<Employee>()
    .FromSnapshot(source)
    .WithRowHashComparison(e => e.RowHash)
    
    .WithBatchSize(3)
    
    .ExcludePropertyFromComparison(e => e.LastUpdatedUtc)
    
    // Do not include "IsAwesome" in list of columns to compare, when detecting changes to records
    .ExcludePropertyFromComparison(e => e.IsAwesome)
    // Also, do not update it, even something else in the record has changed.
    .ExcludeColumnFromUpdate(e => e.IsAwesome)
    
    .WithSoftDeletion(e => e.Deleted)
    
    // Once a record is identified as changed, and is merged into the database table, we want a server side
    // expression to be evaluated for the field.
    .OnUpdateRecordSetExpression(e => e.LastUpdatedUtc, "current_timestamp")
    
    // We want to execute the merge, and capture all changes made (note: this part is slow and holds the database
    // transaction for a longer period of time, in order to capture the changes. Only use if you must. Otherwise use
    // the fire-and-forge ExecuteAsync method.
    .ExecuteAndGetChangesAsync(CancellationToken.None);

await foreach (var change in changes)
{
    Console.WriteLine($"{change.Action}: {change.Record.Id}");
}

Console.WriteLine("All done!");