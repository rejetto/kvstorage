import { createReadStream, createWriteStream, WriteStream } from 'fs'
import { unlink, rename, mkdir, writeFile, appendFile, rm, stat } from 'fs/promises'
import { buffer as stream2buffer, text as stream2string } from 'node:stream/consumers'
import { dirname, join } from 'path'
import { EventEmitter, once } from 'events'
import { pipeline } from 'stream/promises'
import { Transform } from 'stream'
import readline from 'readline'
import { serialize as v8Serialize } from 'v8'

export type Jsonable<EXPAND> = EXPAND | JsonPrimitive | JsonArray<EXPAND> | JsonObject<EXPAND>
type JsonPrimitive = number | boolean | null | string
type JsonObject<EXPAND> = { [key: string]: Jsonable<EXPAND> | undefined } // don't complain about undefined-s that won't be saved
type JsonArray<EXPAND> = Jsonable<EXPAND>[]

type Encodable = undefined | Jsonable<Buffer | Date>
type Reviver = (k: string, v: any) => any
type Encoder = (k: string | undefined, v: any, skip: object) => any

// info related to a value of a specific key (not included)
type MemoryValue<T> = {
    v?: T, offloaded?: number, bucket?: [number, number], file?: string, // mutually exclusive fields: v=DirectValue (in memory), offloaded=OffloadedValue (in main file), bucket=BucketValue (in bucket file), file=ExternalFile (in dedicated file)
    format?: 'json', // only for ExternalFile and BucketValue
    size?: number, // bytes in the main file
    waited?: number, // time already waited on this key
    onDisk?: MemoryValue<T>,  // the record currently written on disk for the same key (may even be self)
    pendingSince?: number, // timestamp of the last put() while it waits to be written
}

type IteratorOptions = { startsWith?: string, limit?: number }
type OptionFields = 'memoryThreshold' | 'bucketThreshold' | 'fileThreshold' | 'rewriteThreshold' | 'rewriteOnOpen'
    | 'rewriteLater' | 'defaultPutDelay' | 'maxPutDelay' | 'maxPutDelayCreate' | 'reviver' | 'encoder' | 'digArrays'
    | 'dontWriteSameValue' | 'fileCollisionSeparator' | 'keyToFileName' | 'cleanupOnOpen'

export type KvStorageOptions<T=Encodable> = Partial<Pick<KvStorage<T>, OptionFields>>

// persistent key-value storage functionality with an API inspired by levelDB
export class KvStorage<T=Encodable> extends EventEmitter {
    // above this number of bytes, value won't be kept in memory, just key
    memoryThreshold = 1_000
    // above this number of bytes, value will be kept in a common bucket file (simple Buffer-s are saved as binaries)
    bucketThreshold = 2_000
    // above this number of bytes, value will be kept in a dedicated file (simple Buffer-s are saved as binaries). This has precedence over bucket
    fileThreshold = 100_000
    // above this percentage (over the file size), a rewrite will be triggered to remove wasted space
    rewriteThreshold = 0.3
    // enable rewrite on open
    rewriteOnOpen = true
    // enable rewrite after open
    rewriteLater = false
    // default delay before writing to file
    defaultPutDelay = 0
    // limit put-delay to avoid indefinite extension, since the delay resets with each put for the same key
    maxPutDelay = 10_000
    // override put delay for missing keys, or keys that weren't written to disk yet
    maxPutDelayCreate?: number
    // passed to JSON.parse
    reviver?: Reviver
    // passed to JSON.stringify
    encoder?: Encoder
    // should encoder recur in array entries
    digArrays = false
    // you can disable this if you want 'put' to be slightly faster, at the cost of extra space. Not effective in case of fileThreshold
    dontWriteSameValue = true
    // must not be one of the chars used in keyToFileName
    fileCollisionSeparator = '~'
    // allow opting out of stale temp-file cleanup during open (eg. debugging or custom external cleanup)
    cleanupOnOpen = true
    // set while opening
    protected opening: Promise<void> | undefined = undefined
    // appended to the prefix of sublevel()
    static subSeparator = '\t'
    protected bucketPath = ''
    // a record exists in memory if it exists on disk
    protected map = new Map<string, MemoryValue<T>>()
    // keep track of the actual number of keys, since deleted keys are in memory until they are discarded from the disk as well
    protected mapRealSize = 0
    protected path = ''
    protected folder = ''
    protected _isOpen = false
    protected isDeleting = false
    protected fileStream: WriteStream | undefined = undefined
    protected bucketStream: WriteStream | undefined = undefined
    // keep track to be able to make offloaded objects
    protected fileSize = 0
    // track size to make fewer I/O operations
    protected bucketSize = 0
    // keep track of how many bytes we would save by rewriting
    protected wouldSave = 0
    protected bucketWouldSave = 0
    // used to avoid parallel writings
    protected lockWrite: Promise<unknown> = Promise.resolve()
    // used to account also for delayed writings
    protected lockFlush: Promise<unknown> = Promise.resolve()
    // keep track, to not issue more than one
    protected rewritePending: undefined | Promise<unknown>
    // keep track, to not issue more than one
    protected rewriteBucketPending: undefined | Promise<unknown>
    // keep track of collision by base-filename, and produce unique filename in the same time of a get+set
    protected files = new Map<string, number>()

    constructor(options: KvStorageOptions<T>={}) {
        super()
        this.setMaxListeners(Infinity) // we may need one for every key
        Object.assign(this, options)
        this.reviver = (k, v) => options.reviver ? options.reviver(k,v)
            : v?.$KV$ === 'Buffer' ? Buffer.from(v.base64, 'base64')
                : v?.$KV$ === 'Date' ? new Date(v.date)
                    : v
    }

    isOpen() { return this._isOpen }

    isOpening() {
        // the main purpose of .then here is to avoid uncaught promises for those calling isOpening, but also to duplicate the promise, as in some cases its reference in global scope prevented it from being g-collected (and a single ctrl+c didn't close the process)
        return this.opening?.then(() => true, () => false)
    }

    async ready() { return this._isOpen || once(this, 'open') }

    async open(path: string, { clear=false }={}) {
        if (!path)
            throw Error("invalid path")
        return this.opening ??= new Promise<void>(async (resolve, reject) => {
            try {
                if (this._isOpen)
                    throw Error("cannot open twice")
                this.path = path
                this.folder = path + '$'
                this.bucketPath = path + '-bucket'
                if (clear)
                    await this.removeStorageFiles().catch(() => {})
                // recover replacement files
                const main = await getReplacementState(this.path)
                const bucket = await getReplacementState(this.bucketPath)
                if (!main.path && main.old && main.new && bucket.path && bucket.old && !bucket.new) {
                    await validateMainFile(newPath(this.path))
                    // bucket rewrite promotes the bucket first; in this state the only consistent recovery is to finish promoting the matching main snapshot
                    await rename(newPath(this.path), this.path)
                    await cleanupReplacementFiles(this.path, this.cleanupOnOpen)
                    await cleanupReplacementFiles(this.bucketPath, this.cleanupOnOpen)
                }
                await recoverReplacementFile(this.path, validateMainFile, this.cleanupOnOpen)
                await recoverReplacementFile(this.bucketPath, undefined, this.cleanupOnOpen)

                this.isDeleting = false
                await this.load()
                if (this.rewriteOnOpen)
                    await this.considerRewrite()
                this.fileStream ??= createWriteStream(path, { flags: 'a' })
                await streamReady(this.fileStream)
                this._isOpen = true
                this.emit('open')
                resolve()
            }
            catch(e) {
                reject(e)
            }
            finally {
                this.opening = undefined
            }
        })
    }

    async close() {
        await this.flush()
        this._isOpen = false // during flush we accept new put() calls, but now the time is over
        await this.flush() // writes accepted during the first flush can update lockFlush after its snapshot, so drain once more before closing streams
        if (this.fileStream)
            await new Promise(res => this.fileStream!.close(res))
        this.fileStream = undefined
        if (this.bucketStream)
            await new Promise(res => this.bucketStream!.close(res))
        this.bucketStream = undefined
        this.map.clear()
    }

    async flush(): typeof this.lockFlush {
        this.emit('flush')
        const current = this.lockFlush // wait lockFlush, because a write may have been delayed and only then lockWrite will be set
        await current
        await this.lockWrite // flushed waits may append several writes to lockWrite before all of them finish
        return this.lockFlush === current || this.flush() // if more puts arrived meanwhile, recur and emit 'flush' again so the new pending writes skip waiting
    }

    async clear() {
        await this.unlink()
        await this.open(this.path)
    }

    async put(key: string, value: T | undefined, { delay=this.defaultPutDelay, maxDelay=this.maxPutDelay, maxDelayCreate=this.maxPutDelayCreate, forceSync=false }={}) {
        if (!this._isOpen)
            throw Error("storage not open")
        const was = this.map.get(key)
        if (!was?.file && was?.offloaded === undefined && !was?.bucket && was?.v === value) return // quick sync check, good for primitive values and objects identity. If you delete a missing value, we'll exit here
        const wasDefined = isMemoryValueDefined(was)
        const willBeDefined = value !== undefined
        // keep size tied to key existence, independently from where the value is stored
        if (wasDefined !== willBeDefined)
            if (willBeDefined)
                this.mapRealSize++
            else
                this.mapRealSize--
        const start = Date.now()
        if (was?.pendingSince) { // truthy = waiting to start writing
            // this is an important optimization for bursts of writes: we update existing promise and objects instead of piling up new ones
            was.v = value
            was.pendingSince = start
            return this.lockFlush
        }
        const will: MemoryValue<T> = { v: value, onDisk: was?.onDisk, waited: was?.waited, pendingSince: start } // keep reference to what's on disk
        this.map.set(key, will)
        const self = this
        if (!was?.onDisk || was?.v === undefined) // maxDelayCreate applies both to values never written and missing values
            maxDelay = maxDelayCreate ?? maxDelay
        const toWait = Math.max(0, Math.min(delay, maxDelay - (was?.waited || 0)))
        return this.lockFlush = this.wait(toWait).then(writeLater)

        function writeLater(flushed = false): Promise<unknown> {
            const totalWaited = (will.waited || 0) + Date.now() - start
            const idleWaited = Date.now() - (will.pendingSince || start)
            if (flushed) // flush() is expected to drain pending puts immediately instead of re-arming the coalescing delay.
                return self.lockWrite = self.lockWrite.then(writeValue)
            // if the key was updated recently, keep stretching the same pending write until idle>=delay or maxDelay is exhausted
            if (idleWaited < delay && totalWaited < maxDelay)
                return self.wait(Math.min(delay - idleWaited, maxDelay - totalWaited)).then(writeLater)
            return self.lockWrite = self.lockWrite.then(writeValue)
        }

        async function writeValue() {
            if (self.isDeleting) return
            if (will.onDisk === will) return // written by a rewrite()
            value = will.v
            will.pendingSince = undefined
            const inMemoryNow = self.map.get(key)
            if (inMemoryNow !== will) { // we were overwritten
                if (inMemoryNow && delay) // keep track of the time already waited on the same key
                    inMemoryNow.waited = (inMemoryNow.waited || 0) + Date.now() - start
                return
            }
            const {folder} = self
            const oldFile = inMemoryNow?.onDisk?.file // don't use `was` as an async writing could have happened in the meantime
            if (oldFile)
                await unlink(join(folder, oldFile))
            const saveExternalFile = async (content: Uint8Array | string, format?: 'json') => {
                let filename = self.keyToFileName(key)
                const n = self.files.get(filename)
                self.files.set(filename, (n || 0) + 1)
                if (n) filename += self.fileCollisionSeparator + n
                const fullPath = join(folder, filename)
                await mkdir(dirname(fullPath), { recursive: true })
                await writeFile(fullPath, content)
                const newRecord = { file: filename, format, onDisk: inMemoryNow } satisfies MemoryValue<T>
                await self.appendRecord(key, newRecord)
            }
            try {
                const asBuffer = value instanceof Uint8Array && value
                if (asBuffer && asBuffer.length > self.fileThreshold) // optimization for simple buffers, but we don't compare with old buffer content
                    return saveExternalFile(asBuffer)
                // compare with value currently on disk
                const encodeValue = (v: T | undefined) => v === undefined ? '' : self.encode(v)
                const {onDisk} = will
                // external records have no inline value to compare, including when deleting
                const encodedOldValue = self.dontWriteSameValue && !onDisk?.file && (
                    await self.readOffloadedEncoded(onDisk) ?? await self.readBucketEncoded(onDisk) ?? encodeValue(onDisk?.v) )
                if (asBuffer && asBuffer.length > self.bucketThreshold)
                    // optimized bucket-buffer comparison
                    return self.dontWriteSameValue && onDisk?.bucket && encodedOldValue instanceof Buffer && encodedOldValue.equals(asBuffer)
                        || self.appendBucket(key, asBuffer)
                const encodedNewValue = encodeValue(value)
                if (self.dontWriteSameValue && encodedNewValue === encodedOldValue) return // unchanged, don't save
                const encodedNewValueSize = getUtf8Size(encodedNewValue)
                if (encodedNewValueSize > self.fileThreshold)
                    return asBuffer ? saveExternalFile(asBuffer) // encoded is bigger, but no reason to not use optimization of simple buffers
                        : saveExternalFile(encodedNewValue!, 'json')
                if (encodedNewValueSize > self.bucketThreshold)
                    return self.appendBucket(key, encodedNewValue)
                const { offset, size } = await self.appendRecord(key, will)
                if (!forceSync && self.map.get(key) === will && getMemorySize(value) > self.memoryThreshold) // don't offload over a newer pending value
                    self.map.set(key, { offloaded: offset, size, onDisk: will.onDisk })
            }
            finally {
                if (value === undefined && self.map.get(key) === will) // don't delete a newer pending value
                    self.map.delete(key)
            }
        }
    }

    async get(key: string) {
        await this.opening
        const rec = this.map.get(key)
        if (!rec) return
        return await this.readExternalFile(rec) // if it is, it's surely not undefined
            ?? await this.readBucketValue(rec)
            ?? await this.readOffloadedValue(rec)
            ?? rec.v
    }

    // for sync use-cases
    getSync(key: string) {
        return this.map.get(key)?.v
    }

    del(key: string) {
        return this.put(key, undefined)
    }

    has(key: string) {
        return isMemoryValueDefined(this.map.get(key))
    }

    async unlink() {
        if (this.isDeleting || !this.path) return
        await this.lockWrite
        this.isDeleting = true
        await this.close()
        await this.removeStorageFiles()
    }

    size() {
        return this.mapRealSize
    }

    async *iterator(options: IteratorOptions={}) {
        await this.opening
        for (const k of this.keys(options))
            yield [k, await this.get(k)]
    }

    *keys(options: IteratorOptions={}) {
        for (const k of KvStorage.filterKeys(this.map.keys(), options, this.map))
            yield k
    }

    firstKey(options: IteratorOptions={}) {
        return KvStorage.filterKeys(this.map.keys(), options, this.map).next().value
    }

    protected static *filterKeys(keys: Iterable<string>, options: IteratorOptions={}, map?: typeof KvStorage.prototype.map) {
        let { startsWith='', limit=Infinity } = options
        for (const k of keys) {
            if (!limit) return
            if (!k.startsWith(startsWith)) continue
            if (map && !isMemoryValueDefined(map.get(k))) continue
            limit--
            yield k
        }
    }

    singleSync<ST extends T>(key: string, def: ST) {
        const self = this
        return {
            async ready() {
                await (self._isOpen || once(self, 'open'))
                const rec = self.map.get(key)
                if (!rec || 'v' in rec || rec.offloaded === undefined) return // only main-file offloads are safe to rehydrate
                const v = await self.readOffloadedValue(rec) as ST
                if (v === undefined) throw Error("singleSync value not ready")
                self.map.set(key, { v, onDisk: rec }) // restore sync reads after memoryThreshold offloaded the value
            },
            get() { return self.getSync(key) as ST ?? def },
            set(v: ST | ((was: ST) => ST)) {
                if (v instanceof Function)
                    v = v(this.get())
                // sync-check; in put it would be async
                if (v && (typeof v === 'string' || typeof v === 'object'))
                    for (const x of [getUtf8Size(self.encode(v)), v instanceof Uint8Array && v.length || 0])
                        if (x > self.fileThreshold)
                            throw Error("singleSync value exceeds fileThreshold")
                        else if (x > self.bucketThreshold)
                            throw Error("singleSync value exceeds bucketThreshold")
                self.put(key, v, { forceSync: true })
                return v
            },
            toJSON() { return this.get() },
        }
    }

    async asObject() {
        const ret: any = {}
        for await (const [k, v] of this.iterator())
            ret[k] = v
        return ret
    }

    sublevel(prefix: string) {
        const storage = this
        prefix = prefix + KvStorage.subSeparator
        const ret = {
            flush: () => storage.flush(),
            put: (key: string, value: T | undefined) => storage.put(prefix + key, value),
            get: (key: string) => storage.get(prefix + key),
            del: (key: string) => storage.del(prefix + key),
            async unlink() {
                for (const k of Array.from(this.keys({}))) await this.del(k)
            },
            size: () => Array.from(ret.keys({})).length,
            has: (key: string) => storage.has(prefix + key),
            *keys(options: IteratorOptions={}) {
                // A live view over parent keys avoids stale sublevel state when parent writes bypass this sublevel.
                const startsWith = prefix + (options.startsWith || '')
                for (const k of KvStorage.filterKeys(storage.map.keys(), { ...options, startsWith }, storage.map))
                    yield k.slice(prefix.length)
            },
            async *iterator(options: IteratorOptions={}) {
                for (const k of this.keys(options))
                    yield [k, await this.get(k)]
            },
            sublevel: (childPrefix: string) => storage.sublevel(prefix + childPrefix),
        }
        return ret
    }

    keyToFileName(key: string) {
        const filename = key.replace(/[^\w./]/g, '').slice(0, 10)
        // path-control segments must not reach join(), while ordinary dots and subfolders stay supported
        return filename.split('/').filter(segment => segment && segment !== '.' && segment !== '..').join('/') || 'f'
    }

    protected wait(t: number) {
        return new Promise<boolean>(resolve => {
            if (t <= 0) return resolve(false)
            let h: any
            const cleanStop = (flushed=false) => {
                clearTimeout(h)
                this.removeListener('flush', onFlush)
                resolve(flushed)
            }
            const onFlush = () => cleanStop(true)
            h = setTimeout(cleanStop, t)
            this.on('flush', onFlush)
        })
    }

    protected readBucketEncoded(v: MemoryValue<T> | undefined) {
        if (!v?.bucket) return
        const [o,n] = v.bucket
        const stream = createReadStream(this.bucketPath, { start: o, end: o + n - 1 })
        return v.format === 'json' ? stream2string(stream) : stream2buffer(stream)
    }

    protected async readBucketValue(v: MemoryValue<T> | undefined) {
        const encoded = await this.readBucketEncoded(v)
        if (encoded === undefined) return
        return v?.format === 'json' && typeof encoded === 'string' ? this.decode(encoded) : encoded
    }

    protected readOffloadedEncoded(v: MemoryValue<T> | undefined) {
        if (v?.offloaded === undefined) return
        if (!v.size || v.size < 1) {
            this.emit('errorDecoding', Error(`invalid offloaded metadata: offloaded=${v.offloaded} size=${v.size}`))
            return Promise.resolve('')
        }
        return stream2string(createReadStream(this.path, { start: v.offloaded, end: v.offloaded + v.size - 1 }))
    }

    // limited to 'ready' and 'offloaded'
    protected async readOffloadedValue(mv: MemoryValue<T>) {
        return this.readOffloadedEncoded(mv)?.then(line =>
            (this.decode(line||'') as any)?.v)
    }

    protected decode(data: string): Encodable {
        try { return JSON.parse(data, this.reviver) }
        catch(e) { this.emit('errorDecoding', e) }
    }

    protected async readExternalFile(v: MemoryValue<T>) {
        if (!v?.file) return
        const f = createReadStream(join(this.folder, v.file))
        return v?.format === 'json' ? this.decode(await stream2string(f))
            : stream2buffer(f)
    }

    rewrite() {
        return this.rewritePending ||= this.lockFlush = this.lockWrite = this.lockWrite.then(async () => {
            if (this.isDeleting)
                return this.rewritePending = undefined
            this.emit('rewrite', this.rewritePending)
            const {path} = this
            const rewriting = newPath(path) // use same volume, to be sure we can rename to destination
            if (this.fileStream?.writable)
                await closeStream(this.fileStream)
            await prepareReplacement(path)
            this.fileStream = createWriteStream(rewriting, { flags: 'w' })
            this.fileSize = 0
            this.wouldSave = 0
            for (const k of this.map.keys()) {
                const mv = this.map.get(k)
                if (!mv || 'v' in mv && mv.v === undefined || !mv.onDisk) continue // no value, or not written yet
                const {offset} = await this.appendRecord(k, mv, true)
                if (mv?.size && mv.offloaded !== undefined)
                    mv.offloaded = offset // just offset has changed
            }
            await closeStream(this.fileStream)
            await replaceFile(path)
            this.fileStream = createWriteStream(path, { flags: 'a' })
            await streamReady(this.fileStream)
            this.rewritePending = undefined
            void this.flush()
        })
    }

    rewriteBucket() {
        return this.rewriteBucketPending ||= this.lockFlush = this.lockWrite = this.lockWrite.then(async () => {
            if (this.isDeleting)
                return this.rewriteBucketPending = undefined
            this.emit('rewriteBucket', this.rewriteBucketPending)
            const {bucketPath: path} = this
            const rewriting = newPath(path) // use same volume, to be sure we can rename to destination
            const mainRewriting = newPath(this.path)
            let f: typeof this.bucketStream
            let lastWrite: any
            let ofs = 0
            let mainWouldSave = 0
            const newMap = new Map(this.map)
            await prepareReplacement(path)
            await prepareReplacement(this.path)
            for (const [k, mv] of this.map.entries()) {
                const encoded = await this.readBucketEncoded(mv)
                if (!encoded) continue
                f ??= createWriteStream(rewriting, { flags: 'w' })
                lastWrite = new Promise(res => f!.write(encoded, res))
                const size = mv!.bucket![1]
                const rec: MemoryValue<T> = { bucket: [ofs, size], format: mv!.format }
                rec.onDisk = rec
                newMap.set(k, rec)
                mainWouldSave += mv?.onDisk?.size ?? 0
                ofs += size
            }
            await lastWrite
            if (!f) { // empty
                if (this.bucketStream)
                    await closeStream(this.bucketStream)
                await unlinkWithRetry(path) // keep deletion under lockWrite so it cannot remove a bucket reopened by the next put
                this.bucketStream = undefined
            }
            else {
                if (this.fileStream?.writable)
                    await closeStream(this.fileStream)
                await closeStream(f)
                if (this.bucketStream)
                    await closeStream(this.bucketStream)
                await pipeline(createReadStream(this.path), createWriteStream(mainRewriting, { flags: 'w' }))
                this.fileStream = createWriteStream(mainRewriting, { flags: 'a' })
                this.fileSize = (await stat(mainRewriting)).size
                // append bucket coordinates to db.new so the official main and bucket files switch as a recoverable pair
                for (const [k, v] of newMap.entries())
                    if (v.bucket && v !== this.map.get(k))
                        await this.appendRecord(k, v, true)
                const fileSize = this.fileSize
                await closeStream(this.fileStream)
                await replaceFileForPair(this.path, path)
                this.map = this.mergePendingSnapshot(newMap)
                this.fileSize = fileSize
                this.fileStream = createWriteStream(this.path, { flags: 'a' })
                await streamReady(this.fileStream)
                this.bucketStream = createWriteStream(path, { flags: 'a' })
                await streamReady(this.bucketStream)
            }
            this.wouldSave += mainWouldSave
            this.bucketSize = ofs
            this.bucketWouldSave = 0
            this.rewriteBucketPending = undefined
            if (this.rewriteLater)
                void this.considerRewrite()
        })
    }

    protected async load(): Promise<void> {
        try {
            this.files.clear()
            this.wouldSave = 0 // calculate how much we'd save by rewriting
            this.mapRealSize = this.bucketWouldSave = this.fileSize = this.bucketSize = 0 // reset
            this.map.clear()
            // check if tail contains CR. the first line is checked in the for-loop
            const {size} = await stat(this.path)
            if (size) {
                const lastTerminator = await stream2buffer(createReadStream(this.path, { start: Math.max(0, size - 2), end: size - 1 }))
                if (lastTerminator.includes(13))
                    await this.normalizeMainFile()
                if (lastTerminator[lastTerminator.length - 1] !== 10) {
                    // separate an interrupted append from future records before calculating offsets
                    await appendFile(this.path, '\n')
                    return this.load()
                }
            }

            const input = createReadStream(this.path)
            const rl = readline.createInterface({ input })
            let filePos = 0 // track where we are, to make Offloaded
            let nextFilePos = 0
            let newlineSize = 0
            for await (const line of rl) {
                const lineBytes = getUtf8Size(line)
                filePos = nextFilePos
                if (!newlineSize) {
                    const start = filePos + lineBytes  // filePos is zero, but it's good practice to include it
                    newlineSize = '\r\n' === await stream2string(createReadStream(this.path, { start, end: start + 1 })) ? 2 : 1
                    if (newlineSize === 2) {
                        await new Promise(res => input.close(res)) // On Windows rename can fail while the read handle is still open
                        await this.normalizeMainFile()
                        return this.load()
                    }
                }
                const bytesIncludingNL = lineBytes + newlineSize
                // Crash/truncation may leave the last line without a newline; clamping keeps offsets aligned with on-disk bytes.
                nextFilePos = Math.min(size, filePos + bytesIncludingNL)
                const record = this.decode(line) as any
                if (typeof record?.k !== 'string') { // malformed
                    this.wouldSave += bytesIncludingNL
                    continue
                }
                const { k, v, file, format, bucket } = record
                const valueSize = getMemorySize(v)
                if (file) { // rebuild this.files
                    // we don't rely on using the current keyToFileName, as we allow having used a different one in the past
                    const [base, n] = file.split(this.fileCollisionSeparator)
                    const was = this.files.get(base)
                    const next = (Number(n) || 0) + 1 // files stores the next suffix to allocate, not the highest suffix seen on disk
                    if (!was || next > was)
                        this.files.set(base, next)
                }
                const already = this.map.get(k)
                this.wouldSave += already?.size || 0
                const mv: MemoryValue<T> = {
                    ...file ? { file, format } : bucket ? { bucket, format } : valueSize > this.memoryThreshold ? { offloaded: filePos } : { v },
                    size: lineBytes,
                    onDisk: undefined, // small optimization – V8 should not change the hidden-class in next assignment
                }
                mv.onDisk = mv // we are reading, so that's what's on disk
                this.map.set(k, mv)
                const wasDefined = isMemoryValueDefined(already)
                const nowDefined = isMemoryValueDefined(record)
                if (nowDefined !== wasDefined)
                    if (nowDefined)
                        this.mapRealSize++
                    else
                        this.mapRealSize--
            }
            this.fileSize = nextFilePos
        }
        catch (e: any) {
            if (e?.code !== 'ENOENT') // silent on no-file
                throw e
            await rm(this.folder,  { recursive: true, force: true }) // leftover
        }
    }

    protected async encodeRecord(k: string, mv: MemoryValue<T>) {
        return await this.readOffloadedEncoded(mv) // offloaded will keep the same key. This is acceptable with current usage.
            ?? this.encode({ k, ...mv, onDisk: undefined, waited: undefined, pendingSince: undefined, size: undefined } satisfies MemoryValue<T> & { k: string },
                'v' in mv) // if we got 'v' (not offloaded), it's DirectValue or ExternalFile, and we extra encoding work may be necessary
    }

    // NB: this must be called only from within a lockWrite
    protected async appendRecord(key: string, mv: MemoryValue<T>, rewriting=false) {
        const line = await this.encodeRecord(key, mv)
        // Writing empty/non-string lines would silently inject malformed rows and drift offsets.
        if (typeof line !== 'string' || !line)
            throw Error(`invalid encoded record for key ${JSON.stringify(key)}`)
        const res = await this.appendLine(line)
        this.emit('wrote', { key, rewriting, value: mv.v })
        if (!rewriting && mv !== mv.onDisk) {
            this.wouldSave += mv.onDisk?.size ?? 0
            if (mv && 'v' in mv && mv.v === undefined)
                this.wouldSave += res.size
            this.bucketWouldSave += this.map.get(key)?.onDisk?.bucket?.[1] ?? 0
            if (this.rewriteLater)
                void this.considerRewrite()
        }
        mv.waited = undefined // reset
        mv.size = res.size
        mv.onDisk = mv
        if (!rewriting) {
            const current = this.map.get(key)
            // a put accepted during I/O keeps its value but must compare against the write that just completed
            if (current?.pendingSince)
                current.onDisk = mv
            else
                this.map.set(key, mv)
        }
        return res
    }

    protected async considerRewrite() {
        if (!this.rewriteThreshold) return
        if (this.wouldSave / this.fileSize > this.rewriteThreshold)
            await this.rewrite()
        if (this.bucketWouldSave / this.bucketSize > this.rewriteThreshold)
            await this.rewriteBucket()
    }

    protected encode(v: any, useEncoder=true) {
        if (useEncoder) {
            const skip = Object(false) // a unique value to skip recursion at this point
            const self = this
            v = (function recur(x, k?: string) {
                if (self.encoder) {
                    const res = self.encoder?.(k, x, skip)
                    if (res === skip) return x
                    x = res
                }
                if (!x) return x
                // the classes we encode internally
                if (x instanceof Buffer)
                    return { $KV$: 'Buffer', base64: x.toString('base64') } // base64 is 2.6x more efficient on storage space (on average) than Buffer's default
                if (x instanceof Date)
                    return { $KV$: 'Date', date: x.toJSON() }
                const array = self.digArrays && Array.isArray(x)
                if (x.constructor !== Object && !array)
                    return x
                // lazy shallow-clone
                let ret = x
                for (const [k, v] of Object.entries(x)) {
                    const res = recur(v, k)
                    if (ret === x) {
                        if (res === v) continue
                        ret = array ? [] : {}
                        for (const [pk, pv] of Object.entries(x)) // copy previous entries
                            if (pk === k) break
                            else ret[pk] = pv
                    }
                    ret[k] = res
                }
                return ret
            })(v)
        }
        return JSON.stringify(v)
    }

    protected async appendLine(line: string) {
        const offset = this.fileSize
        const size = getUtf8Size(line)
        this.fileSize += size + 1 // 1=newline
        this.emit('write', line)
        await new Promise(res => this.fileStream!.write(line + '\n', res))
        return { offset, size }
    }

    protected mergePendingSnapshot(snapshot: typeof this.map) {
        const merged = new Map(snapshot)
        for (const [k, mv] of this.map.entries())
            if (mv.pendingSince)
                // pending writes started while the rewrite was running and still need to see themselves in the live map when their queued writer resumes
                merged.set(k, mv)
        return merged
    }

    protected async appendBucket(key: string, what: string | Uint8Array) {
        this.bucketSize ||= await stat(this.bucketPath).then(x => x.size, () => 0)
        this.bucketStream ||= createWriteStream(this.bucketPath, { flags: 'a' })
        this.emit('writeBucket', what)
        await new Promise(res => this.bucketStream!.write(what, res))
        const isString = typeof what === 'string'
        const size = isString ? getUtf8Size(what) : what.length
        const rec: MemoryValue<T> = {
            bucket: [this.bucketSize, size],
            format: isString ? 'json' : undefined,
            onDisk: undefined
        }
        this.bucketSize += size
        await this.appendRecord(key, rec)
    }

    protected async normalizeMainFile() {
        const temp = newPath(this.path)
        await prepareReplacement(this.path)
        await pipeline(
            createReadStream(this.path),
            new Transform({ // we only need canonical newlines in storage files: stripping CR bytes avoids text decoding overhead
                transform(chunk, _, done) {
                    done(undefined, Buffer.from(chunk.toString('latin1').replace(/\r/g, ''), 'latin1')) // latin1 avoids multibyte decode artifacts and lets native String.replace strip CR faster than a JS byte loop
                }
            }),
            createWriteStream(temp, { flags: 'w' })
        )
        await replaceFile(this.path)
    }

    protected async removeStorageFiles() {
        await prepareReplacement(this.path) // recovery must not resurrect values after an explicit clear or unlink
        await prepareReplacement(this.bucketPath)
        await unlink(this.path).catch(() => {})
        await unlink(this.bucketPath).catch(() => {})
        await rm(this.folder,  { recursive: true, force: true })
    }

}

export function getUtf8Size(s: string) {
    return Buffer.from(s).length
}

function getMemorySize(v: unknown) {
    if (v == null) return 0
    if (v instanceof Uint8Array) return v.length
    if (typeof v === 'string') return v.length * 2
    if (typeof v === 'number') return 8
    if (typeof v === 'boolean') return 4
    return v8Serialize(v).byteLength // it gives a stable value-only proxy
}

async function replaceFile(path: string) {
    const old = oldPath(path)
    const haveOld = await rename(path, old) // in case the file is locked by another process, we first make the name available
        .then(() => true, e => {
            if (e.code === 'ENOENT') // not a problem, we mean to delete it anyway
                return false
            throw e
        })
    await rename(newPath(path), path)
    if (haveOld)
        unlinkEventually(old)
}

async function replaceFileForPair(mainPath: string, bucketPath: string) {
    // this order keeps either the old pair or the new pair recoverable at every crash point
    await rename(mainPath, oldPath(mainPath))
    await rename(bucketPath, oldPath(bucketPath))
    await rename(newPath(bucketPath), bucketPath)
    await rename(newPath(mainPath), mainPath)
    unlinkEventually(oldPath(mainPath))
    unlinkEventually(oldPath(bucketPath))
}

async function prepareReplacement(path: string) {
    // fixed replacement names must be clear before a new swap starts, otherwise rename could target a stale snapshot
    await unlinkWithRetry(newPath(path))
    await unlinkWithRetry(oldPath(path))
}

async function recoverReplacementFile(path: string, validateNew?: (path: string) => Promise<unknown>, cleanup=true) {
    const state = await getReplacementState(path)
    if (state.path) {
        await cleanupReplacementFiles(path, cleanup)
        return
    }
    if (state.old) {
        await rename(oldPath(path), path)
        if (cleanup)
            await unlink(newPath(path)).catch(() => {})
        return
    }
    if (!state.new) return
    if (validateNew)
        await validateNew(newPath(path))
    await rename(newPath(path), path)
}

async function cleanupReplacementFiles(path: string, cleanup=true) {
    if (!cleanup) return
    await unlink(oldPath(path)).catch(() => {})
    await unlink(newPath(path)).catch(() => {})
}

async function getReplacementState(path: string) {
    const exists = (p: string) => stat(p).then(() => true, () => false)
    return {
        path: await exists(path),
        old: await exists(oldPath(path)),
        new: await exists(newPath(path)),
    }
}

async function validateMainFile(path: string) {
    const input = createReadStream(path)
    const rl = readline.createInterface({ input })
    for await (const line of rl) {
        const record = JSON.parse(line)
        if (typeof record?.k !== 'string')
            throw Error(`invalid replacement main file: ${path}`)
    }
}

function newPath(path: string) {
    return path + '.new'
}

function oldPath(path: string) {
    return path + '.old'
}

function streamReady(s: WriteStream) {
    if (!s.pending) return
    return new Promise<void>((res, rej) => {
        const done = () => {
            s.off('ready', done)
            s.off('error', fail)
            res()
        }
        const fail = (e: Error) => {
            s.off('ready', done)
            s.off('error', fail)
            rej(e)
        }
        s.once('ready', done)
        s.once('error', fail)
        // very fast opens can clear pending before the listener observes ready
        setImmediate(() => s.pending || done())
    })
}

function closeStream(s: WriteStream) {
    return new Promise<void>((res, rej) => {
        s.once('error', rej)
        s.end(res)
    })
}

function unlinkEventually(path: string) {
    // old snapshots are recoverable leftovers, so transient Windows locks should not make a successful replacement fail
    setTimeout(() => unlinkWithRetry(path).catch(() => {}))
}

async function unlinkWithRetry(path: string) {
    let retry = 4
    while (retry--) {
        const done = await unlink(path)
            .then(() => true, e => e.code === 'ENOENT')
        if (done) return
        await new Promise(res => setTimeout(res, 500))
    }
    await unlink(path)
}

function isMemoryValueDefined(mv: MemoryValue<unknown> | undefined) {
    return mv?.v !== undefined || mv?.offloaded !== undefined || mv?.file || mv?.bucket
}
