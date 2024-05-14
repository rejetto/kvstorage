import { createReadStream, createWriteStream, WriteStream } from 'fs'
import { unlink, rename, mkdir, writeFile, rm } from 'fs/promises'
import { buffer as stream2buffer, text as stream2string } from 'node:stream/consumers'
import { basename, dirname, join } from 'path'
import { EventEmitter } from 'events'
import readline from 'readline'

export type Jsonable<EXPAND> = EXPAND | JsonPrimitive | JsonArray<EXPAND> | JsonObject<EXPAND>
type JsonPrimitive = number | boolean | null | string
type JsonObject<EXPAND> = { [key: string]: Jsonable<EXPAND> }
type JsonArray<EXPAND> = Jsonable<EXPAND>[]

type Encodable = undefined | Jsonable<Buffer>
type Reviver = (k: string, v: any) => any

const FILE_DISALLOWED_CHARS = /[^\w\.]/g
const FILE_COLLISION_SEPARATOR = '$' // must not match \w
if (!FILE_DISALLOWED_CHARS.test(FILE_COLLISION_SEPARATOR)) throw "bad choice"

export interface KvStorageOptions {
    // above this number of bytes, record won't be kept in memory at load
    memoryThreshold?: number
    // above this number of bytes, record won't be kept in the main file (simple Buffer-s are saved as binaries)
    fileThreshold?: number
    // above this percentage (over the file size), a rewrite will be triggered to remove wasted space
    rewriteThreshold?: number
    // enable rewrite on open
    rewriteOnOpen?: boolean
    // enable rewrite after open
    rewriteLater?: boolean
    // passed to JSON.parse
    reviver?: Reviver
    // in case you want to customize the name of the files created by fileThreshold
    keyToFileName?: (key: string) => string
    // default delay before writing to file
    defaultPutDelay?: number
}

type MemoryValue = undefined | {
    v?: Encodable, offset?: number, file?: string, // mutually exclusive fields: v=DirectValue, offset=OffloadedValue, file=ExternalFile
    format?: 'json', // only for ExternalFile
    size?: number, // bytes in the main file
    waited?: number
    w: MemoryValue,  // keep a reference to the record currently written on disk
}

type IteratorOptions = { startsWith?: string, limit?: number }

// persistent key-value storage functionality with an API inspired by levelDB
export class KvStorage extends EventEmitter implements KvStorageOptions {
    protected map = new Map<string, MemoryValue>() // a record exists in memory if it exists on disk
    protected realSize = 0 // keep track of the actual size, since deleted keys are in memory until they are discarded from the disk as well
    protected path = ''
    protected folder = ''
    static subSeparator = ''
    protected isOpen = false
    protected isDeleting = false
    protected writeStream: WriteStream | undefined = undefined
    protected fileSize = 0 // keep track to be able to make offloaded objects
    memoryThreshold = 1_000
    fileThreshold = 10_000
    rewriteThreshold = 0.3
    rewriteOnOpen = true
    rewriteLater = false
    defaultPutDelay = 0
    maxPutDelay = 10_000
    wouldSave = 0 // keep track of how many bytes we would save by rewriting
    reviver?: Reviver
    protected lockWrite: Promise<unknown> = Promise.resolve() // used to avoid parallel writings
    protected lockFlush: Promise<unknown> = Promise.resolve() // used to account also for delayed writings
    protected rewritePending: undefined | Promise<unknown> // keep track, to not issue more than one
    protected files = new Map<string, number>() // keep track of collision by base-filename

    constructor(options: KvStorageOptions={}) {
        super()
        this.setMaxListeners(Infinity) // we may need one for every key
        Object.assign(this, options)
        this.reviver = (k,v) => options.reviver ? options.reviver(k,v)
            : v?.$KV$ === 'Buffer' ? Buffer.from(v.base64, 'base64')
            // detect standard serialized buffer
            : v?.type === 'Buffer' && Object.keys(v).length === 2 && Array.isArray(v.data) ? Buffer.from(v.data)
            : v
    }

    async open(path: string, { clear=false }={}) {
        if (this.isOpen)
            throw "cannot open twice"
        this.path = path
        this.folder = path + '$'
        if (clear)
            await this.unlink().catch(() => {})
        this.isDeleting = false
        await this.load()
        if (this.rewriteOnOpen && this.getWasted() > (this.rewriteThreshold || 1))
            await this.rewrite()
        this.writeStream = createWriteStream(this.path, { flags: 'a' })
        this.isOpen = true
    }

    async close() {
        await this.flush()
        this.writeStream = undefined
        this.isOpen = false
        this.map.clear()
    }

    flush() {
        this.emit('flush')
        let current: any
        return current = this.lockFlush.then(async () => {
            if (this.lockFlush !== current) // more could happen in the meantime
                await this.lockFlush
        })
    }

    put(key: string, value: Encodable, { delay=this.defaultPutDelay, maxDelay=this.maxPutDelay }={}) {
        if (!this.isOpen)
            throw "storage must be open first"
        const was = this.map.get(key)
        if (!was?.file && !was?.offset && was?.v === value) return // quick sync check, good for primitive values and objects identity. If you delete a missing value, we'll exit here
        const will: MemoryValue = { v: value, w: was?.w, waited: was?.waited } // keep reference to what's on disk
        this.map.set(key, will)
        if (value === undefined)
            this.realSize--
        else if (was?.v === undefined)
            this.realSize++
        const start = Date.now()
        const toWait = Math.max(0, Math.min(delay, maxDelay - (was?.waited || 0)))
        return this.lockFlush = this.wait(toWait).then(() => this.lockWrite = this.lockWrite.then(async () => {
            if (this.isDeleting) return
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
                if (n) filename += FILE_COLLISION_SEPARATOR + n
                await mkdir(folder).catch(() => {})
                await writeFile(join(folder, filename), content)
                const newRecord = { file: filename, format, w: inMemoryNow } as const
                await this.appendRecord(key, newRecord)
                this.map.set(key, newRecord) // offload
            }
            const isBuffer = value instanceof Buffer
            if (isBuffer && value.length > this.fileThreshold) // optimization for simple buffers, but we don't compare with old buffer content
                return saveExternalFile(value)
            const encodeValue = (v: Encodable) => v === undefined ? '' : JSON.stringify(v)
            const encodedOldValue = await this.readOffloadedEncoded(was) ?? encodeValue(was?.v)
            const encodedNewValue = encodeValue(value)
            if (encodedNewValue === encodedOldValue) return // unchanged, don't save
            if (encodedNewValue?.length! > this.fileThreshold)
                return isBuffer ? saveExternalFile(value) // encoded is bigger, but no reason to not use optimization of simple buffers
                    : saveExternalFile(encodedNewValue!, 'json')
            const { offset, size } = await this.appendRecord(key, will)
            if (size > this.memoryThreshold) // once written, consider offloading
                this.map.set(key, { offset, size, w: will.w })
        }))
    }

    async get(key: string) {
        const rec = this.map.get(key)
        if (!rec) return
        return await this.readExternalFile(rec) // if it is, it's surely not undefined
            ?? await this.readOffloadedValue(rec)
            ?? rec.v
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

    sublevel(prefix: string) {
        prefix = prefix + KvStorage.subSeparator
        const subKeys = new Set(this.keys({ startsWith: prefix }))
        const ret = {
            flush: () => this.flush(),
            put: (key: string, value: Encodable) => {
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

    // modifies the way the buffer is serialized, to be more efficient
    b64(b: Buffer) {
        b.toJSON = bufferJsonAsBase64 as any // ts complains because returned object has different form
        return b
    }

    keyToFileName(key: string) {
        return key.replace(FILE_DISALLOWED_CHARS, '').slice(0, 10)
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

    protected readOffloadedEncoded(v: MemoryValue) {
        return v?.offset === undefined ? undefined : stream2string(createReadStream(this.path, { start: v.offset, end: v.offset + v.size! - 1 }))
    }

    // limited to 'ready' and 'offloaded'
    protected async readOffloadedValue(mv: MemoryValue) {
        return this.readOffloadedEncoded(mv)?.then(line =>
            (this.decode(line||'') as any)?.v)
    }

    protected decode(data: string): Encodable {
        return data ? JSON.parse(data, this.reviver) : undefined
    }

    protected async readExternalFile(v: MemoryValue) {
        if (!v?.file) return
        const f = createReadStream(join(this.folder, v.file))
        return v?.format === 'json' ? this.decode(await stream2string(f))
            : stream2buffer(f)
    }

    protected rewrite() {
        return this.rewritePending ||= this.lockWrite = this.lockWrite.then(async () => {
            this.emit('rewrite')
            const {path} = this
            const randomId = Math.random().toString(36).slice(2, 5)
            const rewriting = join(dirname(path), basename(path) + '-rewriting-' + randomId) // use same volume, to be sure we can rename to destination
            this.writeStream = createWriteStream(rewriting, { flags: 'w' })
            this.fileSize = 0
            this.wouldSave = 0
            for (const k of this.map.keys()) {
                const mv = this.map.get(k)
                if (mv === undefined || 'v' in mv && mv.v === undefined) continue
                const {offset} = await this.appendRecord(k, mv, true)
                if (mv?.size)
                    mv.offset = offset // just offset has changed
            }
            await new Promise(res => this.writeStream!.close(res))
            const deleting = path + '-deleting-' + randomId
            await rename(path, deleting)
            await rename(rewriting, path)
            await unlink(deleting)
            this.rewritePending = undefined
            void this.flush()
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
                const {k, v, file, format} = record
                if (file) { // rebuild this.files
                    let [base, n] = file.split(FILE_COLLISION_SEPARATOR)
                    const was = this.files.get(base)
                    n = Number(n) || 0
                    if (!was || n > was)
                        this.files.set(base, n)
                }
                this.wouldSave += this.map.get(k)?.size || 0
                const mv: MemoryValue = {
                    ...file ? { file, format } : valueSize > this.memoryThreshold ? { offset: filePos } : { v },
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

    protected async encodeRecord(k: string, mv: MemoryValue) {
        return await this.readOffloadedEncoded(mv) // offloaded will keep same key. This is acceptable with current usage.
            ?? JSON.stringify({ k, ...mv, w: undefined, waited: undefined, size: undefined }); // if it's not offloaded, it's DirectValue or ExternalFile
    }

    // NB: this must be called only from within a lockWrite
    protected async appendRecord(key: string, mv: NonNullable<MemoryValue>, rewriting=false) {
        const line = await this.encodeRecord(key, mv)
        const res = await this.appendLine(line)
        this.emit('wrote', { key, rewriting, value: mv.v })
        if (!rewriting && mv !== mv.w) {
            this.wouldSave += mv.w?.size ?? 0
            if (mv && 'v' in mv && mv.v === undefined)
                this.wouldSave += res.size
            if (this.rewriteLater && this.getWasted() > this.rewriteThreshold)
                void this.rewrite()
        }
        mv.waited = undefined // reset
        mv.size = res.size
        mv.w = mv
        return res
    }

    protected async appendLine(line: string) {
        const offset = this.fileSize
        const size = getUtf8Size(line)
        this.fileSize += size + 1 // newline
        await new Promise(res => this.writeStream!.write(line + '\n', res))
        return { offset, size }
    }

    getWasted() {
        return this.wouldSave / this.fileSize
    }
}

// 2.6x more efficient on storage space (on average) than Buffer's default
function bufferJsonAsBase64(this: Buffer) {
    return { $KV$: 'Buffer', base64: this.toString('base64') }
}

export function getUtf8Size(s: string) {
    return Buffer.from(s).length
}
