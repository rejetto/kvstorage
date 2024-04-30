# KvStorage

- Persistent key-value storage
- API inspired by levelDB
- All keys are kept in memory
- Zero dependencies
- Concurrency is not supported

This class was designed to store mostly small data sets of JSON-able data types plus Buffer.
Small values are kept in memory (configurable threshold), while big ones are loaded on-demand.

# File format

Data is stored as a single text file, with each line a json, containing "k" property for they key, and "v" property for the value.
Both new records and updates are appended.
By default (configurable), values bigger than 10KB are kept in a separate file, and instead of "v" you will find a "file" property, with the name of the file.
If you delete a record, then a json with only "k" is appended, so that the value is undefined.
Updates of existing keys cause wasted space, so the file is rewritten if enough percentage of wasted space is found on open.

# Ideas

- diff updates? if an object gets a change in a key, save the diff. Maybe save the key as array.
