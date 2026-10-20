const assert = require('node:assert/strict')
const { mkdtemp, rm, mkdir, writeFile, readFile } = require('node:fs/promises')
const { tmpdir } = require('node:os')
const { join, relative, dirname } = require('node:path')
const { execFileSync } = require('node:child_process')
const { KvStorage } = require('.')

module.exports = async function(measure) {
    const resources = []
    try {
        await measure('another instance cannot open or clear an owned database', async () => {
            const { kv, path } = await storage()
            const other = new KvStorage({ rewriteOnOpen: false })
            try {
                await kv.put('key', 'confirmed')
                await assert.rejects(other.open(path, { clear: true }), /already open/)
                await other.close()
                await assert.rejects(other.open(relative(process.cwd(), path)), /already open/)
                await kv.close()
                await other.open(path)
                assert.equal(await other.get('key'), 'confirmed')
                await kv.close()
                await assert.rejects(kv.open(path), /already open/)
            }
            finally { await other.close() }
        })

        await measure('a database is reserved while its first open is still pending', async () => {
            const { kv, path } = await storage()
            await kv.close()
            const other = new KvStorage({ rewriteOnOpen: false })
            const opening = kv.open(path)
            try {
                await assert.rejects(other.open(path), /already open/)
                await opening
                assert.equal(kv.isOpen(), true)
            }
            finally {
                await opening
                await other.close()
            }
        })

        await measure('a failed open releases the path and allows both instances to retry', async () => {
            const { kv, path } = await storage()
            await kv.close()
            const missing = join(path + '-missing', 'data')
            const other = new KvStorage({ rewriteOnOpen: false })
            try {
                await assert.rejects(kv.open(missing), { code: 'ENOENT' })
                await mkdir(dirname(missing))
                await other.open(missing)
                await other.put('key', 'confirmed')
                await other.close()
                await kv.open(missing)
                assert.equal(await kv.get('key'), 'confirmed')
                await kv.put('key', 'updated')
            }
            finally { await other.close() }
        })

        for (const value of [1, 2]) {
            await measure(`awaiting a coalesced put waits for its key (value ${value})`, async () => {
                const { kv, path } = await storage()
                let persisted = false
                kv.on('wrote', ({ key }) => { if (key === 'a') persisted = true })
                const first = kv.put('a', 1, { delay: 100 })
                const other = kv.put('b', 1)
                await kv.put('a', value)
                assert.equal(persisted, true)
                assert.equal(JSON.parse((await readFile(path, 'utf8')).trim().split('\n').pop()).v, value)
                await Promise.all([first, other])
                await kv.close()
                await kv.open(path)
                assert.equal(await kv.get('a'), value)
            })
        }

        await measure('a pending update followed by rewrite does not block later updates', async () => {
            const { kv, path } = await storage()
            await kv.put('key', 1)
            const pending = kv.put('key', 2, { delay: 100 })
            await kv.rewrite()
            await pending
            await kv.put('key', 3)
            await kv.close()
            await kv.open(path)
            assert.equal(await kv.get('key'), 3)
        })

        await measure('failed encoding preserves an external value and permits subsequent writes', async () => {
            const failure = Error('cannot encode this value')
            const { kv, path } = await storage({
                encoder(key, value, skip) {
                    if (value === 'invalid') throw failure
                    return skip
                },
            })
            const original = Buffer.alloc(100_001, 'a')
            await kv.put('key', original)
            await assert.rejects(kv.put('key', 'invalid'), error => error === failure)
            assert.deepEqual(await kv.get('key'), original)
            await kv.put('other', 'valid')
            await kv.close()
            await kv.open(path)
            assert.deepEqual(await kv.get('key'), original)
            await kv.put('key', 'recovered')
            await kv.close()
            await kv.open(path)
            assert.equal(await kv.get('other'), 'valid')
            assert.equal(await kv.get('key'), 'recovered')
        })

        await measure('failed external file creation preserves the last confirmed value', async () => {
            const { kv, path } = await storage()
            const original = Buffer.alloc(100_001, 'a')
            await kv.put('key', original)
            await writeFile(join(path + '$', 'blocked'), 'regular file')
            kv.keyToFileName = () => 'blocked/value'
            await assert.rejects(kv.put('key', Buffer.alloc(100_001, 'b')), error => ['ENOTDIR', 'EEXIST'].includes(error.code))
            await kv.close()
            await kv.open(path)
            assert.deepEqual(await kv.get('key'), original)
        })

        await measure('a failed encoder does not prevent retrying the same value', async () => {
            let failing = true
            const { kv, path } = await storage({
                encoder(key, value, skip) {
                    if (failing) throw Error('temporary encoding failure')
                    return skip
                },
            })
            await assert.rejects(kv.put('key', 'value'), /temporary encoding failure/)
            assert.ok(!kv.has('key'))
            assert.equal(kv.size(), 0)
            failing = false
            await kv.put('key', 'value')
            await kv.close()
            await kv.open(path)
            assert.equal(await kv.get('key'), 'value')
            assert.equal(kv.size(), 1)
        })

        await measure('an encoder failure does not block writes to another key', async () => {
            const { kv, path } = await storage({
                encoder(key, value, skip) {
                    if (value === 'invalid') throw Error('encoding failure')
                    return skip
                },
            })
            await kv.put('bad', 'confirmed')
            await assert.rejects(kv.put('bad', 'invalid'), /encoding failure/)
            await kv.put('good', 'valid')
            await kv.close()
            await kv.open(path)
            assert.equal(await kv.get('bad'), 'confirmed')
            assert.equal(await kv.get('good'), 'valid')
        })

        await measure('adversarial: queued compaction cannot restore a rejected value', async () => {
            let failing = true
            const { kv, path } = await storage({
                encoder(key, value, skip) {
                    if (failing && value === 'retry') throw Error('encoding failure')
                    return skip
                },
            })
            await kv.put('bucket', Buffer.alloc(3_000, 'b'))
            const failed = assert.rejects(kv.put('key', 'retry'), /encoding failure/)
            // let the writer enter the queue, then place compaction immediately behind it
            await Promise.resolve()
            const rewriting = kv.rewriteBucket()
            await failed
            await rewriting
            assert.ok(!kv.has('key'), 'compaction must not resurrect the failed write')
            assert.equal(kv.size(), 1)
            failing = false
            await kv.put('key', 'retry')
            await kv.close()
            await kv.open(path)
            assert.equal(await kv.get('key'), 'retry')
        })

        await measure('adversarial: flush waits for compaction queued during another compaction', async () => {
            const { kv } = await storage()
            await kv.put('bucket', Buffer.alloc(3_000, 'b'))
            let next
            let completed = false
            // queue the second operation after flush has started waiting for the first
            kv.once('wrote', () => {
                next = kv.rewriteBucket().then(() => { completed = true })
            })
            const first = kv.rewrite()
            await kv.flush()
            const drained = completed
            await Promise.all([first, next])
            assert.ok(drained, 'flush returned before the queued compaction completed')
        })

        await measure('adversarial: close drains a chain of compactions before releasing storage', async () => {
            const { kv, path } = await storage()
            const value = Buffer.alloc(3_000, 'b')
            await kv.put('bucket', value)
            const compactions = []
            let completed = 0
            // alternate compactions so each pass can queue more work while close is draining
            kv.on('wrote', ({ rewriting }) => {
                if (!rewriting || compactions.length === 2) return
                const operation = compactions.length ? kv.rewrite() : kv.rewriteBucket()
                const tracked = operation.then(() => { completed++ })
                void tracked.catch(() => {})
                compactions.push(tracked)
            })
            const first = kv.rewrite()
            await kv.close()
            const drained = completed === 2
            await Promise.all([first, ...compactions])
            assert.ok(drained, 'close returned before the compaction chain completed')
            await kv.open(path)
            assert.deepEqual(await kv.get('bucket'), value)
        })

        await measure('close drains other accepted writes before reporting an encoder failure', async () => {
            const { kv, path } = await storage({
                encoder(key, value, skip) {
                    if (value === 'invalid') throw Error('encoding failure')
                    return skip
                },
            })
            const failed = assert.rejects(kv.put('bad', 'invalid', { delay: 100 }), /encoding failure/)
            const valid = kv.put('good', 'value', { delay: 100 })
            await assert.rejects(kv.close(), /encoding failure/)
            await Promise.all([failed, valid])
            await kv.open(path)
            assert.equal(await kv.get('good'), 'value')
            assert.ok(!kv.has('bad'))
        })

        for (const kind of ['external', 'inline', 'bucket', 'delete']) {
            await measure(`interruption before the ${kind} journal record preserves an external value`, async () => {
                const { kv, path } = await storage()
                await kv.close()
                execFileSync(process.execPath, ['-e', `
                    const { KvStorage } = require(process.argv[1])
                    ;(async () => {
                        const kv = new KvStorage({ rewriteOnOpen: false })
                        await kv.open(process.argv[2])
                        await kv.put('key', Buffer.alloc(100_001, 'a'))
                        // the public event fires immediately before appending the replacement record
                        kv.once('write', () => process.exit(0))
                        const kind = process.argv[3]
                        await kv.put('key', kind === 'external' ? Buffer.alloc(100_001, 'b')
                            : kind === 'bucket' ? Buffer.alloc(3_000, 'b')
                            : kind === 'inline' ? 'small' : undefined)
                        throw Error('interruption was not reached')
                    })().catch(e => { console.error(e); process.exit(1) })
                `, require.resolve('.'), path, kind], { timeout: 10_000 })
                await kv.open(path)
                assert.deepEqual(await kv.get('key'), Buffer.alloc(100_001, 'a'), kind)
            })
        }

        await measure('a bucket stream error rejects put and flush without an unhandled error event', async () => {
            const { kv, path } = await storage()
            await mkdir(path + '-bucket')
            await assert.rejects(kv.put('key', Buffer.alloc(3_000)), { code: 'EISDIR' })
            await assert.rejects(kv.flush(), { code: 'EISDIR' })
            await assert.rejects(kv.put('other', 'valid'), { code: 'EISDIR' })
            await assert.rejects(kv.close(), { code: 'EISDIR' })
            assert.equal(kv.isOpen(), false)
            await rm(path + '-bucket', { recursive: true })
            await kv.open(path)
            await kv.put('other', 'valid')
            await kv.close()
            await kv.open(path)
            assert.equal(await kv.get('other'), 'valid')
        })

        if (process.platform !== 'win32')
            await measure('a journal write exceeding the file size limit rejects and preserves confirmed data', async () => {
                const { kv, path } = await storage()
                await kv.close()
                // limit only the child process so a real partial filesystem write fails without affecting the test runner
                execFileSync('/bin/sh', ['-c', 'ulimit -f 1 || exit 1; exec "$@"', 'kvstorage-limit', process.execPath, '-e', `
                    const assert = require('node:assert/strict')
                    const { KvStorage } = require(process.argv[1])
                    process.on('SIGXFSZ', () => {})
                    ;(async () => {
                        const kv = new KvStorage({ rewriteOnOpen: false })
                        await kv.open(process.argv[2])
                        await kv.put('stable', 'confirmed')
                        await assert.rejects(kv.put('large', 'x'.repeat(1_500)), { code: 'EFBIG' })
                        await assert.rejects(kv.flush(), { code: 'EFBIG' })
                        await assert.rejects(kv.put('next', 'value'), { code: 'EFBIG' })
                        await assert.rejects(kv.close(), { code: 'EFBIG' })
                        assert.equal(kv.isOpen(), false)
                    })().catch(e => { console.error(e); process.exit(1) })
                `, require.resolve('.'), path], { timeout: 10_000 })
                await kv.open(path)
                assert.equal(await kv.get('stable'), 'confirmed')
                assert.ok(!kv.has('large'))
                await kv.put('next', 'value')
                await kv.close()
                await kv.open(path)
                assert.equal(await kv.get('next'), 'value')
            })

        await measure('a failed rewrite rejects subsequent writes until close and reopen', async () => {
            const { kv, path } = await storage()
            await kv.put('key', 'confirmed')
            await mkdir(path + '.new')
            await assert.rejects(kv.rewrite())
            await assert.rejects(kv.put('key', 'unconfirmed'))
            await assert.rejects(kv.flush())
            await assert.rejects(kv.close())
            await rm(path + '.new', { recursive: true })
            await kv.open(path)
            assert.equal(await kv.get('key'), 'confirmed')
            await kv.put('key', 'recovered')
            await kv.close()
            await kv.open(path)
            assert.equal(await kv.get('key'), 'recovered')
        })

        await measure('asObject preserves __proto__ as an ordinary own property', async () => {
            const { kv, path } = await storage()
            await kv.put('__proto__', { example: true })
            await kv.close()
            await kv.open(path)
            const result = await kv.asObject()
            assert.equal(Object.getPrototypeOf(result), Object.prototype)
            assert.ok(Object.getOwnPropertyDescriptor(result, '__proto__'))
            assert.deepEqual(result.__proto__, { example: true })
        })

        await measure('proto serialization preserves own properties when encoding dates', async () => {
            for (const nested of [false, true]) {
                const { kv, path } = await storage()
                const value = JSON.parse('{"__proto__":{"flag":true}}')
                const date = new Date('2026-01-02T03:04:05.000Z')
                if (nested)
                    value.__proto__.date = date
                else
                    value.date = date
                await kv.put('object', value)
                await kv.close()
                await kv.open(path)
                const restored = await kv.get('object')
                assert.equal(Object.getPrototypeOf(restored), Object.prototype)
                assert.ok(Object.getOwnPropertyDescriptor(restored, '__proto__'))
                assert.deepEqual(restored, value)
            }
        })
    }
    finally {
        for (const { kv, folder } of resources) {
            await kv.close().catch(() => {})
            await rm(folder, { recursive: true, force: true })
        }
    }

    async function storage(options = {}) {
        const folder = await mkdtemp(join(tmpdir(), 'kvstorage-writes-'))
        const path = join(folder, 'data')
        const kv = new KvStorage({ rewriteOnOpen: false, ...options })
        resources.push({ kv, folder })
        await kv.open(path)
        return { kv, path }
    }
}

if (require.main === module)
    module.exports(async (name, run) => {
        await run()
        console.log('OK', name)
    }).catch(error => { console.error(error); process.exitCode = 1 })
