using Microsoft.EntityFrameworkCore;

namespace SqlServerSample1;

public class Employee
{
    public int Id { get; set; }
    public required string Name { get; set; }
    public DateTime LastUpdatedUtc { get; set; }
    public bool IsAwesome { get; set; }
    public long Version { get; set; }
    public Guid RowHash { get; set; }
    public bool Deleted { get; set; }
}

public class MyDbContext : DbContext
{
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
    public MyDbContext(DbContextOptions<MyDbContext> options)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        : base(options)
    {
        
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Employee>(e =>
        {
            e.ToTable("MyEmployees");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id)
                .HasColumnName("ID")
                .ValueGeneratedNever()
                .IsRequired();
            e.Property(x => x.Name)
                .IsRequired()
                .HasMaxLength(100)
                .HasColumnName("DisplayName");

            e.Property(x => x.LastUpdatedUtc)
                .HasColumnName("UpdatedUtc");
            e.Property(x => x.RowHash)
                .HasColumnName("__Hash");
        });
    }

    public DbSet<Employee> Employees { get; set; }
}