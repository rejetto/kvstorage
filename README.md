# KvStorage

- Persistent key-value storage
- Node.js only, 16+ (no browser)
- API inspired by levelDB
- All keys are kept in memory
- Zero dependencies
- Small bundle size (9KB minified)
- Typescript + Javascript
- Concurrency is not supported
- Throttled writings to file

This class was designed to store small/medium data sets of JSON-able data types plus Buffer,
where you can have the luxury of keeping keys in memory.
Small values are kept in memory as well (configurable threshold), while big ones are loaded on-demand.
You can fine-tune this to match your memory-usage vs performance on reading.

# File format

Data is stored as a single text file, with each line a json, containing "k" property for they key, 
and "v" property for the value. 

Both new records and updates are appended. If you delete a record, 
then a json with only "k" is appended, so that the value is undefined.

```json
{"k":"a","v":1}
{"k":"b","v":2}
{"k":"a","v":1.1}  // this is an update
{"k":"c","v":3}
{"k":"b"}          // this is a deletion
```

Comments are not part of the file, here just for clarity. The whole file is not a valid JSON, but each separate line is. 

## File size
Updates of existing keys cause wasted space, so the file is rewritten when wasting a configurable percentage of space.
You can limit this rewriting to load-time, if you don't want to risk performance degrading later.
By delaying put()s you can reduce wasted space, in case same key is rewritten in the meantime. 

To keep the main file small and keep operations fast, we have 2 mechanisms:

- bucket file

  the bucket file is a common file that store big values (by default is >10KB), while the key stays in the main file.
  In the main file, instead of "v" you will find a "bucket" property, with coordinates inside the bucket file.
  
- dedicated files 

  By default, values bigger than 100KB (configurable) are kept in a separate file, while the key stays in the main file  
  together with a "file" property with the relative path of the file. All these files are kept in a folder.

This is all transparent to you, just keep using `get` and `put` without worrying.

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

The `any` below actually means a value that can be JSON-encoded (plus Buffer).

- `constructor(options?)`
  - options:
    - memoryThreshold: Above this number of bytes, a record won't be kept in memory at load (default: 1000).
    - bucketThreshold: Above this number of bytes, a record will be stored in a separate but common file, called bucket (simple Buffers are saved as binaries) (default: 10000).
    - fileThreshold: Above this number of bytes, a record will be stored in a dedicated file (simple Buffers are saved as binaries) (default: 100000).
    - rewriteThreshold: Above this percentage (over the file size), a rewrite will be triggered at load time, to remove wasted space (default: 0.3).
    - rewriteOnOpen: Enable rewriteThreshold on open().
    - rewriteLater: Enable rewriteThreshold after open.
    - defaultPutDelay: default value for `put(k,v,{delay})`.
    - maxPutDelay: default value for `put(k,v,{maxDelay})`.
    - reviver: A function passed to JSON.parse for custom deserialization (optional).
    - keyToFileName: A function to customize the name of the files created by fileThreshold (optional). Including path-separator(s) in the name you can divide files in subfolders.
- `open(path: string): Promise<void>`
  - Opens the key-value store at the specified path. If clear is true, existing data will be deleted. 
- `get(key: string): Promise<any>`
- `put(key: string, value: any, { delay, maxDelay }?): Promise<void>`
  - add/overwrite a value. Await-ing this is optional, unless you need to know that the file has been written.
  - Adding a delay may save some writings for fast-changing values. 
- `del(key: string): Promise<void>`
  - remove a value. Await-ing this is optional, unless you need to know that the file has been written.
- `has(key: string): boolean`
  - check for existence 
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
- `clear(): Promise<void>`
  - Equivalent to unlink + open. 
- `b64(Buffer): Buffer`
    - Modifies a Buffer object to be serialized efficiently in a JSON as base64, 2.6x smaller than default behavior.
    - E.g. `db.put('someKey', db.b64(myBuffer) )`

# Ideas

- diff updates? if an object gets a change in a key, save the diff.
