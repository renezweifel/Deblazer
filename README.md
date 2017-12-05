# Deblazer ORM
## PreRelease version for NetStandard2.0
At the moment no prerelease build on nuget.org and no tooling for usage.
Build and tooling follow later, when version more stable and tested

Deblazer is the C# ORM written and used by [Digitec Galaxus](https://github.com/DigitecGalaxus) it has query language similiar to LINQ.

## Getting Started
Install the latest package from [Nuget](https://www.nuget.org/packages/Deblazer/)

### Usuage
Use the [artifacts tooling](https://github.com/DigitecGalaxus/Deblazer.Artifacts) to generate the necessary artifacts classes.
```cs
var db = new Db("My Connection String");

var top10people = db.Application_Peoples()
    .TakeDb(10)
    .ToList();
```

## Development
Deblazer is written in C# and targets the .NET 4.6.1 Framework
