# KvStorage

- Persistent key-value storage
- Node.js only (no browser)
- API inspired by levelDB
- All keys are kept in memory
- Zero dependencies
- Small bundle size (6KB minified)
- Concurrency is not supported

This class was designed to store mostly small data sets of JSON-able data types plus Buffer.
Small values are kept in memory (configurable threshold), while big ones are loaded on-demand.

# File format

Data is stored as a single text file, with each line a json, containing "k" property for they key, and "v" property for the value.
Both new records and updates are appended.
```json
{"k":"a","v":1}
{"k":"b","v":2}
{"k":"a","v":1.1}
{"k":"c","v":3}
```
By default, values bigger than 10KB (configurable) are kept in a separate file, and instead of "v" you will find a "file" property, with the name of the file.
If you delete a record, then a json with only "k" is appended, so that the value is undefined.
Updates of existing keys cause wasted space, so the file is rewritten if enough percentage of wasted space is found on open.

# Buffer

There are some optimizations for class Buffer:
- if the value is just a buffer (not nested) and bigger than the threshold, it will be saved without JSON-encoding
- a `b64` method is offered to be space-efficient when the buffer is inside another object

# API

## By example

```javascript
const { KvStorage } = require('@rejetto/kvstorage')
let db = new KvStorage()
db.open('filename')
db.put('frank', { age: 33 })
setTimeout(async () => {
    console.log(await db.get('frank'))
    db.del('frank')
})
```

## Methods

where we say `any` below, we actually mean values that can be JSON-encoded (plus Buffer)

- `constructor(options?)`
  - options:
    - memoryThreshold: Above this number of bytes, a record won't be kept in memory at load (default: 1000).
    - fileThreshold: Above this number of bytes, a record won't be kept in the main file (simple Buffers are saved as binaries) (default: 10000).
    - rewriteThreshold: Above this percentage (over the file size), a rewrite will be triggered at load time, to remove wasted space (default: 0.3).
    - reviver: A function passed to JSON.parse for custom deserialization (optional).
    - keyToFileName: A function to customize the name of the files created by fileThreshold (optional).
- `open(path: string): Promise<void>`
  - Opens the key-value store at the specified path. If clear is true, existing data will be deleted. 
- `get(key: string): Promise<any>`
- `put(key: string, value: any): Promise<void>`
    - await-ing this is optional, unless you need to know that the file has been written
- `del(key: string): Promise<void>`
    - await-ing this is optional, unless you need to know that the file has been written
- `size(): number`
  - Returns the number of key-value pairs stored.
- `*iterator(options?): AsyncGenerator<[key, value]>`
  - Iterates over key-value pairs with optional filtering.
- `*keys(options?): Generator<string>`
  - Iterates over keys with optional filtering.
- `sublevel(prefix: string): KvStorage-like`
  - Creates a sublevel key-value store with a specific prefix.
- `close(): Promise<void>`
  - Closes the key-value store, releasing resources. 
- `flush(): Promise<void>`
  - Flushes any pending writes to disk.
- `unlink(): Promise<void>`
  - Deletes the entire key-value store at its location.
- `b64(Buffer): Buffer`
    - Modifies a Buffer object to be serialized efficiently in a JSON as base64, 2.6x smaller than default behavior.

# Ideas

- diff updates? if an object gets a change in a key, save the diff. Maybe save the key as array.
