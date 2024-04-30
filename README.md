# KvStorage

Persistent key-value storage functionality with an API inspired by levelDB.
All keys are kept in memory. Concurrency is not supported.

This class has been designed to mostly store small data sets, that are JSON-able (plus Buffer).
Small values (configurable) are kept in memory, while big ones are loaded on-demand.

# File format

File format is text, with each line a json with "k" property for they key, and "v" property for the value.
New records, and updates, are always appended.
By default, values with 10KB of size are kept in a separate file, and instead of "v" you will find a "file" property, with the name of the file.
If you delete a record, then a json with only "k" is appended, so that the value is undefined.
Updates of existing keys cause wasted space, so the file is rewritten if enough percentage of wasted space is found on open.

# Ideas

- diff updates? if an object gets a change in a key, save the diff. Maybe save the key as array.
