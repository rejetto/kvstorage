import { createReadStream, createWriteStream, WriteStream } from 'fs'
import { unlink, rename, mkdir, writeFile, rm, stat } from 'fs/promises'
import { buffer as stream2buffer, text as stream2string } from 'node:stream/consumers'
import { dirname, join } from 'path'
import { EventEmitter, once } from 'events'
import readline from 'readline'

export type Jsonable<EXPAND> = EXPAND | JsonPrimitive | JsonArray<EXPAND> | JsonObject<EXPAND>
type JsonPrimitive = number | boolean | null | string
type JsonObject<EXPAND> = { [key: string]: Jsonable<EXPAND> | undefined } // don't complain for undefined-s that will not saved
type JsonArray<EXPAND> = Jsonable<EXPAND>[]

type Encodable = undefined | Jsonable<Buffer | Date>
type Reviver = (k: string, v: any) => any
type Encoder = (k: string | undefined, v: any, skip: object) => any

export interface KvStorageOptions {
    // above this number of bytes, value won't be kept in memory, just key
    memoryThreshold?: number
    // above this number of bytes, value will be kept in a common bucket file (simple Buffer-s are saved as binaries)
    bucketThreshold?: number
    // above this number of bytes, value will be kept in a dedicated file (simple Buffer-s are saved as binaries)
    fileThreshold?: number
    // above this percentage (over the file size), a rewrite will be triggered to remove wasted space
    rewriteThreshold?: number
    // enable rewrite on open
    rewriteOnOpen?: boolean
    // enable rewrite after open
    rewriteLater?: boolean
    // passed to JSON.parse
    reviver?: Reviver
    // passed to JSON.stringify
    encoder?: Encoder
    // should encoder recur in array entries
    digArrays?: boolean
    // in case you want to customize the name of the files created by fileThreshold
    keyToFileName?: (key: string) => string
    // default delay before writing to file
    defaultPutDelay?: number
    // limit put-delay to avoid indefinite extension, since the delay resets with each put for the same key
    maxPutDelay?: number
    // override put delay for missing keys, or keys that weren't written to disk yet
    maxPutDelayCreate?: number
}

type MemoryValue<T> = {
    v?: T, offloaded?: number, bucket?: [number, number], file?: string, // mutually exclusive fields: v=DirectValue, offloaded=OffloadedValue, bucket=BucketValue, file=ExternalFile
    format?: 'json', // only for ExternalFile and BucketValue
    size?: number, // bytes in the main file
    waited?: number
    w?: MemoryValue<T>,  // keep a reference to the record currently written on disk
}

type IteratorOptions = { startsWith?: string, limit?: number }

// persistent key-value storage functionality with an API inspired by levelDB
export class KvStorage<T=Encodable> extends EventEmitter implements KvStorageOptions {
    protected map = new Map<string, MemoryValue<T>>() // a record exists in memory if it exists on disk
    protected realSize = 0 // keep track of the actual number of keys, since deleted keys are in memory until they are discarded from the disk as well
    protected path = ''
    protected folder = ''
    static subSeparator = ''
    protected _isOpen = false
    protected isDeleting = false
    protected fileStream: WriteStream | undefined = undefined
    protected bucketStream: WriteStream | undefined = undefined
    protected fileSize = 0 // keep track to be able to make offloaded objects
    protected bucketSize = 0
    memoryThreshold = 1_000
    bucketThreshold = 10_000
    fileThreshold = 100_000 // this has precedence over bucket
    rewriteThreshold = 0.3
    rewriteOnOpen = true
    rewriteLater = false
    dontWriteSameValue = true // not effective in case of fileThreshold
    defaultPutDelay = 0
    maxPutDelay = 10_000
    maxPutDelayCreate?: number
    wouldSave = 0 // keep track of how many bytes we would save by rewriting
    bucketWouldSave = 0
    bucketPath = ''
    fileCollisionSeparator = '~' // must not be one of the chars used in keyToFileName
    reviver?: Reviver
    encoder?: Encoder
    digArrays = false
    protected lockWrite: Promise<unknown> = Promise.resolve() // used to avoid parallel writings
    protected lockFlush: Promise<unknown> = Promise.resolve() // used to account also for delayed writings
    protected rewritePending: undefined | Promise<unknown> // keep track, to not issue more than one
    protected rewriteBucketPending: undefined | Promise<unknown> // keep track, to not issue more than one
    protected files = new Map<string, number>() // keep track of collision by base-filename, and produce unique filename in the same time of a get+set

    constructor(options: KvStorageOptions={}) {
        super()
        this.setMaxListeners(Infinity) // we may need one for every key
        Object.assign(this, options)
        this.reviver = (k, v) => options.reviver ? options.reviver(k,v)
            : v?.$KV$ === 'Buffer' ? Buffer.from(v.base64, 'base64')
                : v?.$KV$ === 'Date' ? new Date(v.date)
                    : v
    }

    isOpen() { return this._isOpen }

    async open(path: string, { clear=false }={}) {
        if (this._isOpen)
            throw "cannot open twice"
        this.path = path
        this.folder = path + '$'
        this.bucketPath = path + '-bucket'
        if (clear)
            await this.unlink().catch(() => {})
        this.isDeleting = false
        await this.load()
        if (this.rewriteOnOpen)
            await this.considerRewrite()
        this.fileStream ||= createWriteStream(this.path, { flags: 'a' })
        this._isOpen = true
        this.emit('open')
    }

    async close() {
        await this.flush()
        this.fileStream = undefined
        this._isOpen = false
        this.map.clear()
    }

    flush(): typeof this.lockFlush {
        this.emit('flush')
        const current = this.lockFlush // wait lockFlush, because a write may have been delayed and only then lockWrite will be set
        return current.then(() => this.lockFlush === current || this.flush()) // more could happen in the meantime
    }

    async clear() {
        await this.unlink()
        await this.open(this.path)
    }

    put(key: string, value: T | undefined, { delay=this.defaultPutDelay, maxDelay=this.maxPutDelay, maxDelayCreate=this.maxPutDelayCreate }={}) {
        if (!this._isOpen)
            throw "storage must be open first"
        const was = this.map.get(key)
        if (!was?.file && !was?.offloaded && !was?.bucket && was?.v === value) return // quick sync check, good for primitive values and objects identity. If you delete a missing value, we'll exit here
        const will: MemoryValue<T> = { v: value, w: was?.w, waited: was?.waited } // keep reference to what's on disk
        this.map.set(key, will)
        if (value === undefined)
            this.realSize--
        else if (was?.v === undefined)
            this.realSize++
        const start = Date.now()
        if (!was?.w || was?.v === undefined) // maxDelayCreate applies both to values never written and missing values
            maxDelay = maxDelayCreate ?? maxDelay
        const toWait = Math.max(0, Math.min(delay, maxDelay - (was?.waited || 0)))
        return this.lockFlush = this.wait(toWait).then(() => this.lockWrite = this.lockWrite.then(async () => {
            if (this.isDeleting) return
            if (will.w === will) return // wrote by a rewrite
            const inMemoryNow = this.map.get(key)
            if (inMemoryNow !== will) { // we were overwritten
                if (inMemoryNow && delay) // keep track of the time already waited on the same key
                    inMemoryNow.waited = (inMemoryNow.waited || 0) + Date.now() - start
                return
            }
            const {folder} = this
            const oldFile = inMemoryNow?.w?.file // don't use `was` as an async writing could have happened in the meantime
            if (oldFile)
                await unlink(join(folder, oldFile))
            const saveExternalFile = async (content: Buffer | string, format?: 'json') => {
                let filename = this.keyToFileName(key)
                const n = this.files.get(filename)
                this.files.set(filename, (n || 0) + 1)
                if (n) filename += this.fileCollisionSeparator + n
                const fullPath = join(folder, filename)
                await mkdir(dirname(fullPath), { recursive: true })
                await writeFile(fullPath, content)
                const newRecord = { file: filename, format, w: inMemoryNow } as const
                await this.appendRecord(key, newRecord)
                this.map.set(key, newRecord) // offload
            }
            const isBuffer = value instanceof Buffer
            if (isBuffer && value.length > this.fileThreshold) // optimization for simple buffers, but we don't compare with old buffer content
                return saveExternalFile(value)
            // compare with value currently on disk
            const encodeValue = (v: T | undefined) => v === undefined ? '' : this.encode(v)
            const {w} = will
            const encodedOldValue = this.dontWriteSameValue && (
                await this.readOffloadedEncoded(w) ?? await this.readBucketEncoded(w) ?? encodeValue(w?.v) )
            if (isBuffer && value.length > this.bucketThreshold)
                // optimized bucket-buffer comparison
                return this.dontWriteSameValue && w?.bucket && encodedOldValue instanceof Buffer && value.equals(encodedOldValue)
                    || this.appendBucket(key, value)
            const encodedNewValue = encodeValue(value)
            if (this.dontWriteSameValue && encodedNewValue === encodedOldValue) return // unchanged, don't save
            if (encodedNewValue?.length! > this.fileThreshold)
                return isBuffer ? saveExternalFile(value) // encoded is bigger, but no reason to not use optimization of simple buffers
                    : saveExternalFile(encodedNewValue!, 'json')
            if (encodedNewValue?.length! > this.bucketThreshold)
                return this.appendBucket(key, encodedNewValue)
            const { offset, size } = await this.appendRecord(key, will)
            if (size > this.memoryThreshold) // once written, consider offloading
                this.map.set(key, { offloaded: offset, size, w: will.w })
        }))
    }

    async get(key: string) {
        const rec = this.map.get(key)
        if (!rec) return
        return await this.readExternalFile(rec) // if it is, it's surely not undefined
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
        return this.map.has(key)
    }

    async unlink() {
        if (this.isDeleting || !this.path) return
        this.isDeleting = true
        await this.close()
        await unlink(this.path).catch(() => {})
        await unlink(this.bucketPath).catch(() => {})
        await rm(this.folder,  { recursive: true, force: true })
    }

    size() {
        return this.realSize
    }

    async *iterator(options: IteratorOptions={}) {
        for (const k of this.keys(options))
            yield [k, await this.get(k)]
    }

    *keys(options: IteratorOptions={}) {
        for (const k of KvStorage.filterKeys(this.map.keys(), options))
            yield k
    }

    protected static *filterKeys(keys: Iterable<string>, options: IteratorOptions={}) {
        let { startsWith='', limit=Infinity } = options
        for (const k of keys) {
            if (!limit) return
            if (!k.startsWith(startsWith)) continue
            limit--
            yield k
        }
    }

    singleSync<ST extends T>(key: string, def?: ST) {
        const self = this
        const ret = {
            ready: async () => self._isOpen || once(self, 'open'),
            get: () => self.getSync(key) as ST,
            set(v: ST | ((was: ST) => ST)) {
                if (v instanceof Function)
                    v = v(this.get())
                self.put(key, v)
                return v
            },
        }
        if (def !== undefined)
            ret.ready().then(() => ret.set(x => x ?? def))
        return ret
    }

    async asObject() {
        const ret: any = {}
        for await (const [k, v] of this.iterator())
            ret[k] = v
        return ret
    }

    sublevel(prefix: string) {
        prefix = prefix + KvStorage.subSeparator
        const subKeys = new Set(this.keys({ startsWith: prefix }))
        const ret = {
            flush: () => this.flush(),
            put: (key: string, value: T | undefined) => {
                subKeys.add(key)
                this.put(prefix + key, value)
            },
            get: (key: string) => this.get(prefix + key),
            del: (key: string) => {
                subKeys.delete(key)
                return this.del(prefix + key)
            },
            async unlink() {
                for (const k of subKeys) await this.del(k)
            },
            size: () => subKeys.size,
            has: (key: string) => subKeys.has(key),
            *keys(options: IteratorOptions) {
                for (const k of KvStorage.filterKeys(subKeys, options))
                    yield k
            },
            async *iterator(options: IteratorOptions={}) {
                for (const k of this.keys(options))
                    yield [k, await this.get(k)]
            },
            sublevel: (prefix: string) => this.sublevel.call(ret, prefix),
        }
        return ret
    }

    keyToFileName(key: string) {
        return key.replace(/[^\w./]/g, '').slice(0, 10) || 'f'
    }

    protected wait(t: number) {
        return new Promise<void>(resolve => {
            if (t <= 0) return resolve()
            let h: any
            const cb = () => {
                clearTimeout(h)
                this.removeListener('flush', cb)
                resolve()
            }
            h = setTimeout(cb, t)
            this.on('flush', cb)
        })
    }

    protected readBucketEncoded(v: MemoryValue<T> | undefined) {
        if (!v?.bucket) return
        const [o,n] = v.bucket
        const stream = createReadStream(this.bucketPath, { start: o, end: o + n - 1 })
        return v.format === 'json' ? stream2string(stream) : stream2buffer(stream)
    }

    protected readOffloadedEncoded(v: MemoryValue<T> | undefined) {
        return v?.offloaded === undefined ? undefined : stream2string(createReadStream(this.path, { start: v.offloaded, end: v.offloaded + v.size! - 1 }))
    }

    // limited to 'ready' and 'offloaded'
    protected async readOffloadedValue(mv: MemoryValue<T>) {
        return this.readOffloadedEncoded(mv)?.then(line =>
            (this.decode(line||'') as any)?.v)
    }

    protected decode(data: string): Encodable {
        return data ? JSON.parse(data, this.reviver) : undefined
    }

    protected async readExternalFile(v: MemoryValue<T>) {
        if (!v?.file) return
        const f = createReadStream(join(this.folder, v.file))
        return v?.format === 'json' ? this.decode(await stream2string(f))
            : stream2buffer(f)
    }

    rewrite() {
        return this.rewritePending ||= this.lockFlush = this.lockWrite = this.lockWrite.then(async () => {
            this.emit('rewrite', this.rewritePending)
            const {path} = this
            const randomId = Math.random().toString(36).slice(2, 5)
            const rewriting = path + '-rewriting-' + randomId // use same volume, to be sure we can rename to destination
            this.fileStream = createWriteStream(rewriting, { flags: 'w' })
            this.fileSize = 0
            this.wouldSave = 0
            for (const k of this.map.keys()) {
                const mv = this.map.get(k)
                if (!mv || 'v' in mv && mv.v === undefined || !mv.w) continue // no value, or not written yet
                const {offset} = await this.appendRecord(k, mv, true)
                if (mv?.size)
                    mv.offloaded = offset // just offset has changed
            }
            await unlink(path)
            await rename(rewriting, path)
            this.rewritePending = undefined
            void this.flush()
        })
    }

    rewriteBucket() {
        return this.rewriteBucketPending ||= this.lockFlush = this.lockWrite = this.lockWrite.then(async () => {
            this.emit('rewriteBucket')
            const {bucketPath: path} = this
            const randomId = Math.random().toString(36).slice(2, 5)
            const rewriting = path + '-rewriting-' + randomId // use same volume, to be sure we can rename to destination
            const f = createWriteStream(rewriting, { flags: 'w' })
            let lastWrite: any
            let ofs = 0
            const newMap = new Map(this.map)
            for (const [k, mv] of this.map.entries()) {
                const encoded = await this.readBucketEncoded(mv)
                if (!encoded) continue
                lastWrite = new Promise(res => f.write(encoded, res))
                const size = mv!.bucket![1]
                const rec: MemoryValue<T> = { bucket: [ofs, size], format: mv!.format }
                rec.w = rec
                newMap.set(k, rec)
                ofs += size
            }
            await lastWrite
            for (const [k, v] of newMap.entries())
                if (v.bucket)
                    await this.appendRecord(k, v)
            await unlink(path)
            await rename(rewriting, path)
            this.map = newMap
            this.bucketStream = f
            this.bucketSize = ofs
            this.bucketWouldSave = 0
            this.rewritePending = undefined
        })
    }

    protected async load() {
        try {
            const rl = readline.createInterface({ input: createReadStream(this.path) })
            let filePos = 0 // track where we are, to make Offloaded
            let nextFilePos = 0
            this.files.clear()
            this.wouldSave = 0 // calculate how much we'd save by rewriting
            for await (const line of rl) {
                const lineBytes = getUtf8Size(line)
                filePos = nextFilePos
                nextFilePos = filePos + lineBytes + 1 // +newline
                const record = this.decode(line) as any
                if (typeof record?.k !== 'string') continue // malformed
                const wrapSize = getUtf8Size(record.k) + 13 // `{"k":"","v":}`.length
                const valueSize = lineBytes - wrapSize
                const {k, v, file, format, bucket } = record
                if (file) { // rebuild this.files
                    // we don't rely in using current keyToFileName, as we allow having used a different one in the past
                    let [base, n] = file.split(this.fileCollisionSeparator)
                    const was = this.files.get(base)
                    n = Number(n) || 0
                    if (!was || n > was)
                        this.files.set(base, n)
                }
                this.wouldSave += this.map.get(k)?.size || 0
                const mv: MemoryValue<T> = {
                    ...file ? { file, format } : bucket ? { bucket, format } : valueSize > this.memoryThreshold ? { offset: filePos } : { v },
                    size: lineBytes,
                    w: undefined,
                }
                mv.w = mv // we are reading, so that's what's on disk
                this.map.set(k, mv)
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
        return await this.readOffloadedEncoded(mv) // offloaded will keep same key. This is acceptable with current usage.
            ?? this.encode({ k, ...mv, w: undefined, waited: undefined, size: undefined }, 'v' in mv) // if it's not offloaded, it's DirectValue or ExternalFile
    }

    // NB: this must be called only from within a lockWrite
    protected async appendRecord(key: string, mv: MemoryValue<T>, rewriting=false) {
        const line = await this.encodeRecord(key, mv)
        const res = await this.appendLine(line)
        this.emit('wrote', { key, rewriting, value: mv.v })
        if (!rewriting && mv !== mv.w) {
            this.wouldSave += mv.w?.size ?? 0
            if (mv && 'v' in mv && mv.v === undefined)
                this.wouldSave += res.size
            this.bucketWouldSave += this.map.get(key)?.w?.bucket?.[1] ?? 0
            if (this.rewriteLater)
                void this.considerRewrite()
        }
        mv.waited = undefined // reset
        mv.size = res.size
        mv.w = mv
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
                    if (typeof k === 'symbol') continue
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
        this.fileSize += size + 1 // newline
        this.emit('write', line)
        await new Promise(res => this.fileStream!.write(line + '\n', res))
        return { offset, size }
    }

    protected async appendBucket(key: string, what: string | Buffer) {
        this.bucketSize ||= await stat(this.bucketPath).then(x => x.size, () => 0)
        this.bucketStream ||= createWriteStream(this.bucketPath, { flags: 'a' })
        this.emit('writeBucket', what)
        await new Promise(res => this.bucketStream!.write(what, res))
        const isString = typeof what === 'string'
        const size = isString ? getUtf8Size(what) : what.length
        const rec: MemoryValue<T> = {
            bucket: [this.bucketSize, size],
            format: isString ? 'json' : undefined,
            w: undefined
        }
        this.bucketSize += size
        await this.appendRecord(key, rec)
        this.map.set(key, rec)
    }

}

export function getUtf8Size(s: string) {
    return Buffer.from(s).length
}
