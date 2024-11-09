# KvStorage

- Persistent key-value storage
- Node.js only, 16+ (no browser)
- API inspired by levelDB
- All keys are kept in memory
- Zero dependencies
- Small bundle size (10KB minified)
- Typescript + Javascript
- Concurrency is not supported (i.e. no multiple processes on the same file)
- Throttled writings to file

This class was designed to store small/medium data sets of JSON-able data types plus Date and Buffer,
where you can have the luxury of keeping keys in memory.
Small values are kept in memory as well (configurable threshold), while big ones are loaded on-demand.
You can fine-tune this to match your memory-usage vs performance on reading.

# Installation

`npm i @rejetto/kvstorage`

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

  the bucket file is a common file that store big values (by default is >2KB), while the key stays in the main file.
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

Write a value and read it later
```javascript
const { KvStorage } = require('@rejetto/kvstorage')
let db = new KvStorage()
// ...
await db.open('filename')
db.put('frank', { age: 33 })
setTimeout(async () => {
    console.log(await db.get('frank'))
    db.del('frank')
})
```

Same as above, but since we know data is not large, we can go sync 
```javascript
db.put('frank', { age: 33 })
setTimeout(() => {
    console.log(db.getSync('frank'))
    db.del('frank')
})
```

Same as above, but different API 
```javascript
const age = db.singleSync('age', 0)
aget.set(33)
setTimeout(() => {
    console.log(age.get())
})
```

## Methods

The `any` below actually means a value that can be JSON-encoded (plus Date and Buffer).

- `constructor(options?)`
  - options:
    - memoryThreshold: Above this number of bytes, a record is offloaded from memory (default: 1000).
    - bucketThreshold: Above this number of bytes, a record will be stored in a separate but common file, called bucket (simple Buffers are saved as binaries) (default: 10000).
    - fileThreshold: Above this number of bytes, a record will be stored in a dedicated file (simple Buffers are saved as binaries) (default: 100000).
    - rewriteThreshold: Above this percentage (over the file size), a rewrite will be triggered at load time, to remove wasted space (default: 0.3).
    - rewriteOnOpen: Enable rewriteThreshold on open().
    - rewriteLater: Enable rewriteThreshold after open.
    - defaultPutDelay: default value for `put(k,v,{delay})`.
    - maxPutDelay: default value for `put(k,v,{maxDelay})`.
    - maxPutDelayCreate: default value for `put(k,v,{maxDelayCreate})`.
    - reviver: A function passed to JSON.parse for custom deserialization (optional).
    - keyToFileName: A function to customize the name of the files created by fileThreshold (optional). Including path-separator(s) in the name you can divide files in subfolders.
- `open(path: string): Promise<void>`
  - Opens the key-value store at the specified path. If clear is true, existing data will be deleted. 
- `isOpen(): boolean`
  - Current status.
- `isOpening(): undefined | Promise<void>`
  - Set while open() is ongoing.
- `get(key: string): Promise<any>`
  - Get the value associated with the key, or undefined if not present.
- `getSync(key: string): any`
  - Like get, but works only with values that are not offloaded, that are those smaller the memoryThreshold and bucketThreshold and fileThreshold.
- `put(key: string, value: any, { delay, maxDelay, maxDelayCreate }?): Promise<void>`
  - Add/overwrite a value. Await-ing this is optional, unless you need to know that the file has been written.
  - Adding a delay may save some writings for fast-changing values. Since the delay is renewed at each put, we can put a
    limit to that, and can have a generic limit with `maxDelay` or one specific for keys that weren't written to disk yet,
    as you may care more about the existence of a (persisted) record than its updates. 
- `del(key: string): Promise<void>`
  - Remove a value. Await-ing this is optional, unless you need to know that the file has been written.
- `has(key: string): boolean`
  - Check for existence 
- `size(): number`
  - Returns the number of key-value pairs stored.
- `*iterator(options?): AsyncGenerator<[key, value]>`
  - Iterates over key-value pairs with optional filtering.
- `*keys(options?): Generator<string>`
  - Iterates over keys with optional filtering.
- `firstKey(): string`
  - Return first entry of keys.
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
- `asObject(): Promise<Object>`
  -  make all keys and values into a simple Object.
- `singleSync<T=any>(key: string, default: T): { get, set, ready }`
  - Make a simple object-api where you can get and set a value of key without passing it as parameter, and in a sync way.
    Be sure to use this only with values that will not exceed any threshold that will cause it to be offloaded.
  - Default value is not stored, just returned while nothing is stored.
  - Methods:
    - `get(): T` get the value, but sync.
    - `set(value: T | ((currentValue: T) => T)): T` equivalent to `put`, but also offer a callback version 
      to calculate new value based on the old one. Return the new value.
    - `ready(): Promise<void>` so you can wait for the API to be ready to receive commands.

# Ideas

- diff updates? if an object gets a change in a key, save the diff.
