Helper to migrate mappings from ES 2.  Tested with ES 6 upgrade.


docker build -t elasticsearch-migrator:latest .

docker run -it -p 5000:5000 elasticsearch-migrator:latest

Browse the swagger page at http://localhost:5000/

There are two endpoints.
/migrate-indices
This migrates the mappings, settings, and aliases of all valid indices.

/migrate-one/{index}/{doc-type}
Enter the index name and index doc-type to migrate.
Migrated mappings, settings, aliases are output.

TODO
1. Set dynamic to false as default (property at \_doc level)
2. Spring data option to exclude \_class from \_source. Ex: mappings: doc: \_source: excludes: ["\_class"]
3. Map \_parent to join data-type
4. Option to map strings as keyword by default, with a .text suffixed field for text.
