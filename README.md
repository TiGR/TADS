# TiGR's Advanced Data Storage

Stupid name for a simple (and stupid) data storage engine. Kinda NoSQL solution started somewhere around 2005.

## Limits

 - Overhead: minimum 1 byte for most fields.
 - Maximum table size: 4 Gb.
 - Maximum records in table: 4294967296 (32bit). Index size would be 16 Gb. Way too much.
 - Index is number of records * 4 bytes long.
 - Maximum data record length: 1 Mb (specified in _refreshIndex).

## Initial release

 - index ONLY for speedup.
 - ID is the number of record.
 - select single, select range, insert.
 - update ssingle
 - delete single.
 - refresh index.

## Further releases

 - delete all from table.
 - drop table.

## todo

 - mass delete (offset, limit).
 - unique ID - deletion of record should not affect IDs of other records
    (possibly, requires wrapping data into array - null value instead of array
    would mean that record was deleted. Do we need this?!!
    Indexes should contain UID. Each data row also.).
 - file locking, backuping.
 - Consider checkup of idxfile being up-to-date (by comparing mdates of .dat and .idx).

## Already done

 - storing keys and data together.
 - index ONLY for speedup.
 - ID is the # of record.
 - select single.
 - select range(offset, limit, direction[asc/desc]).
 - insert.
 - count (that generally works as MAX()).
 - refresh index.
 - storing keys separately (using *.fmt files).
 - storing types.