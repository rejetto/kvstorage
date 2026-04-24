const { KvStorage } = require('.')
const { statSync, writeFileSync, readFileSync, readdirSync, unlinkSync, existsSync } = require('node:fs')
const { join } = require('path')

test().catch(e => {
    console.error(e)
    process.exit(1)
})

function assert(truth, msg) {
    if (!truth) {
        console.log("FAILED " + msg)
        process.exit(1)
    }
    console.log('OK', msg)
}

async function test() {
    const bytes = []
    for (let i = 0; i < 1001; i++) bytes.push(i % 256) // just above the default memory threshold
    const bufToOffload = Buffer.from(bytes)
    for (let i = 0; i < 20_000 - bufToOffload.length; i++) bytes.push(i % 256)
    const buf = Buffer.from(bytes)
    const bigBuf = Buffer.from(bytes.concat(bytes, bytes, bytes, bytes, bytes))
    const FN = 'test.db'
    let db = new KvStorage()
    let decodeErrors = 0
    db.on('errorDecoding', () => decodeErrors++)
    const original = db.keyToFileName
    db.keyToFileName = k => {
        const x = original(k)
        return join(x[0], x)
    }
    try {
        await measure('all', async () => {
            await measure('basics', async () => {
                assert(!db.isOpening(), 'before isOpening')
                db.open(FN, { clear: true })
                let isReady = false
                db.ready().then(() => isReady = true)
                assert(db.isOpening(), 'while isOpening')
                assert(!isReady, 'not ready')
                await db.isOpening()
                await new Promise(res => res()); assert(isReady, 'ready') // need await because the then() is executed at next tick
                assert(db.isOpen(), "isOpen")
                assert(db.size() === 0, "empty")
                assert(db.firstKey() === undefined, "no firstKey")
                db.put('k1', 'v1')
                assert(await db.get('k1') === 'v1', "no-await")
                db.put('k2', 2)
                db.put('delete', 3)
                db.del('delete')
                db.put('b', bigBuf)
                assert(db.has('b'), "has")
                assert(!db.has('delete'), "has deleted")
                assert(!db.has('never'), "has never")
                assert(!db.firstKey({ startsWith: 'delete' }), "deleted firstKey")
                await db.put('jb', { bigBuf })
                db.put('k2', 22)
                let n = 0
                let valueFound = false
                for await (const [k,v] of db.iterator({ startsWith: 'k' })) {
                    n++
                    valueFound ||= k === 'k2' && v === 22
                }
                assert(valueFound, "iterator value")
                assert(n === 2, "iterator length")
                const sub1 = db.sublevel('P1')
                const date = new Date()
                sub1.put('k under 1', 11)
                sub1.put('k2 under 1', 2)
                sub1.put('k2 under 1', date)
                const sep = KvStorage.subSeparator
                assert(await db.get(`P1${sep}k under 1`), "sub from above")
                assert(await sub1.get('k under 1'), "sub from inside")
                assert(sub1.size() === 2, "sub size")
                assert(Array.from(sub1.keys()).length === 2, "sub length")
                const sub2 = sub1.sublevel('P2')
                sub2.put('k under 2', 21)
                sub2.put('k2 under 2', 22)
                assert(await db.get(`P1${sep}P2${sep}k under 2`), "sub2 from above")
                assert(sub1.size() === 4, "sub father size")
                assert(sub2.size() === 2, "sub child size")
                assert(await sub2.get('k under 2') === 21, "sub get")
                await db.put('off', bufToOffload)
                assert(!db.getSync('off'), 'memory offloaded')
                assert(bufToOffload.equals(await db.get('off')), 'get offloaded')
                // reopen to check persistency
                await db.close()
                const expectedSize = 9
                assert(db.size() === expectedSize, "bad size")
                db = new KvStorage({ rewriteLater: true })
                db.on('errorDecoding', () => decodeErrors++)
                db.on('rewrite', () => {
                    console.log('rewriting to save', db.wouldSave.toLocaleString())
                    db.put('while-rewriting', 1)
                    const t = Date.now()
                    db.lockFlush.then(() => console.log('rewrite finished in', Date.now() - t, 'ms'))
                })
                db.on('rewriteBucket', () =>
                    console.log('rewriting bucket to save', db.bucketWouldSave.toLocaleString()))
                assert(! readdirSync('.').filter(x => x.startsWith(FN + '-win-')).length, "rewrite leftovers") // these would accumulate in time (until process exit)
                writeFileSync(FN, readFileSync(FN, 'utf8').replaceAll('\n', '\r\n')) // resist to editors messing with new lines
                await db.open(FN)
                // Open canonicalizes storage files to LF to keep offsets stable for offloaded records.
                assert(!readFileSync(FN, 'utf8').includes('\r'), 'normalized to LF')
                assert(db.size() === expectedSize, "bad size after reload")
                assert(await db.get('k1') === 'v1', "put+get")
                assert(await db.get('k2') === 22, "numbers")
                assert(await db.get('delete') === undefined, "delete")
                assert(db.getSync(`P1${sep}k2 under 1`).getTime() === date.getTime(), "Date+getSync")
                assert(db.map.get('b')?.file, "binary file")
                assert(db.map.get('jb')?.file, "json file")
                assert((await db.get('b'))?.toString('base64') === bigBuf.toString('base64'), "buffer")
                assert((await db.get('jb'))?.bigBuf?.toString('base64') === bigBuf.toString('base64'), "json-buffer")
                assert(!db.getSync('off'), 'offloaded after open')
                assert(bufToOffload.equals(await db.get('off')), 'get offloaded after open')
                await db.del('off')
            })
            await measure('mixed-newline-regression', async () => {
                const FN = 'mixed-newline.db'
                const targetValue = 'X'.repeat(30)
                const targetLine = JSON.stringify({ k: 'target', v: targetValue })
                const chunks = [JSON.stringify({ k: 'first', v: 1 }) + '\r\n']
                for (let i = 0; i < targetLine.length; i++)
                    chunks.push(JSON.stringify({ k: 'f' + i, v: i }) + '\n')
                chunks.push(targetLine)
                writeFileSync(FN, chunks.join(''))
                const kv = new KvStorage({ memoryThreshold: 1, rewriteOnOpen: false })
                kv.on('errorDecoding', () => decodeErrors++)
                await kv.open(FN)
                await kv.rewrite()
                // This sequence previously produced size=0 offloaded records and then ERR_OUT_OF_RANGE on put().
                await kv.put('target', 'Y')
                assert(await kv.get('target') === 'Y', 'mixed newline rewrite+put')
                await kv.unlink()
            })
            await measure('offloaded-key-apis-regression', async () => {
                const kv = new KvStorage({ memoryThreshold: 1 })
                await kv.open('offloaded-key-apis.db', { clear: true })
                await kv.put('offloaded', 'xx')
                await kv.flush()
                assert(await kv.get('offloaded') === 'xx', 'offloaded get')
                assert(kv.has('offloaded'), 'offloaded has')
                assert(Array.from(kv.keys()).includes('offloaded'), 'offloaded keys')
                assert((await kv.asObject()).offloaded === 'xx', 'offloaded asObject')
                await kv.unlink()
            })
            await measure('offloaded-size-overwrite-regression', async () => {
                const kv = new KvStorage({ memoryThreshold: 1 })
                await kv.open('offloaded-size-overwrite.db', { clear: true })
                await kv.put('offloaded', 'xx')
                assert(kv.size() === 1, 'offloaded size after create')
                await kv.put('offloaded', 'yy')
                assert(kv.size() === 1, 'offloaded size after overwrite')
                await kv.del('offloaded')
                assert(kv.size() === 0, 'offloaded size after delete')
                await kv.unlink()
            })
            await measure('memory-threshold-value-size-regression', async () => {
                const kv = new KvStorage({ memoryThreshold: 10 })
                await kv.open('memory-threshold-value-size.db', { clear: true })
                const longKey = 'long-key-name'.repeat(5)
                await kv.put(longKey, 'x')
                // memoryThreshold is intended to measure the value, not the storage record wrapper
                assert(kv.getSync(longKey) === 'x', 'memory threshold ignores key and wrapper bytes')
                await kv.put('large', 'x'.repeat(11))
                assert(await kv.get('large') === 'x'.repeat(11), 'offloaded value still reads after memory-size check')
                await kv.close()
                await kv.open('memory-threshold-value-size.db')
                assert(kv.getSync(longKey) === 'x', 'memory threshold ignores wrapper bytes after reload')
                assert(await kv.get('large') === 'x'.repeat(11), 'offloaded value still reads after reload')
                await kv.unlink()
            })
            await measure('single-sync-force-sync-regression', async () => {
                const FN2 = 'single-sync-force-sync.db'
                const value = 'x'.repeat(100)
                let kv = new KvStorage({ memoryThreshold: 10, bucketThreshold: 1_000_000, fileThreshold: 1_000_000 })
                await kv.open(FN2, { clear: true })
                const state = kv.singleSync('state', '')
                await state.ready()
                state.set(value)
                await kv.flush()
                assert(kv.getSync('state') === value, 'singleSync ignores memoryThreshold after flush')
                await kv.close()
                kv = new KvStorage({ memoryThreshold: 10, bucketThreshold: 1_000_000, fileThreshold: 1_000_000 })
                await kv.open(FN2)
                const reloaded = kv.singleSync('state', '')
                await reloaded.ready()
                assert(reloaded.get() === value, 'singleSync ready reloads memory-offloaded value')
                await kv.unlink()
            })
            await measure('single-sync-falsy-ready-regression', async () => {
                let kv = new KvStorage({ memoryThreshold: 1, bucketThreshold: 1_000_000, fileThreshold: 1_000_000 })
                await kv.open('single-sync-falsy-ready.db', { clear: true })
                const state = kv.singleSync('state', 1)
                state.set(0)
                await kv.flush()
                await kv.close()
                kv = new KvStorage({ memoryThreshold: 1, bucketThreshold: 1_000_000, fileThreshold: 1_000_000 })
                await kv.open('single-sync-falsy-ready.db')
                const reloaded = kv.singleSync('state', 1)
                await reloaded.ready()
                assert(reloaded.get() === 0, 'singleSync ready reloads falsy offloaded value')
                await kv.unlink()
            })
            await measure('single-sync-force-sync-threshold-regression', async () => {
                const kv = new KvStorage({ memoryThreshold: 1, bucketThreshold: 10, fileThreshold: 1_000_000 })
                await kv.open('single-sync-force-sync-threshold.db', { clear: true })
                const state = kv.singleSync('state', '')
                let threw = false
                try { state.set('x'.repeat(20)) }
                catch { threw = true }
                assert(threw, 'singleSync throws above bucketThreshold')
                await kv.unlink()
            })
            await measure('single-sync-ready-skips-bucket-regression', async () => {
                const FN2 = 'single-sync-ready-bucket-rewrite.db'
                let kv = new KvStorage({ memoryThreshold: 1_000_000, bucketThreshold: 1, fileThreshold: 1_000_000, rewriteOnOpen: false })
                await kv.open(FN2, { clear: true })
                await kv.put('state', 'first bucket payload')
                await kv.flush()
                await kv.close()
                kv = new KvStorage({ memoryThreshold: 1_000_000, bucketThreshold: 1, fileThreshold: 1_000_000, rewriteOnOpen: false })
                await kv.open(FN2)
                const state = kv.singleSync('state', '')
                await state.ready()
                assert(state.get() === '', 'singleSync ready does not rehydrate bucket value')
                assert(await kv.get('state') === 'first bucket payload', 'singleSync ready leaves bucket value readable async')
                await kv.unlink()
            })
            await measure('bucket-json-decode-regression', async () => {
                const kv = new KvStorage({ bucketThreshold: 1 })
                await kv.open('bucket-json-decode.db', { clear: true })
                const text = 'abcdef'
                await kv.put('string', text)
                await kv.put('object', { text })
                assert(await kv.get('string') === text, 'bucket string decodes')
                assert((await kv.get('object')).text === text, 'bucket object decodes')
                await kv.unlink()
            })
            await measure('truncated-tail-regression', async () => {
                const FN = 'truncated-tail.db'
                const kv = new KvStorage({ memoryThreshold: 1, rewriteOnOpen: false })
                let localDecodeErrors = 0
                kv.on('errorDecoding', () => localDecodeErrors++)
                await kv.open(FN, { clear: true })
                const initialValue = 'S'.repeat(30)
                const updatedValue = 'T'.repeat(30)
                await kv.put('stable', initialValue)
                await kv.put('tail', 'U'.repeat(30))
                await kv.close()
                const fileBytes = readFileSync(FN)
                // Dropping the final 5 bytes keeps the file mostly valid while guaranteeing an incomplete trailing record.
                writeFileSync(FN, fileBytes.subarray(0, fileBytes.length - 5))
                await kv.open(FN)
                await kv.put('stable', updatedValue)
                assert(await kv.get('stable') === updatedValue, 'truncated tail keeps offloaded writes working')
                assert(localDecodeErrors > 0, `truncated tail detected ${localDecodeErrors}`)
                await kv.unlink()
            })
            await measure('clear-after-close-stale-rewrite-regression', async () => {
                const FN = 'clear-after-close-stale-rewrite.db'
                const kv = new KvStorage({
                    rewriteLater: false,
                    rewriteOnOpen: true,
                    memoryThreshold: 1_000_000,
                    bucketThreshold: 1_000_000,
                    fileThreshold: 1_000_000,
                })
                await kv.open(FN, { clear: true })
                await kv.put('stable', 'x'.repeat(1000), { delay: 0, maxDelay: 0, maxDelayCreate: 0 })
                await kv.flush()
                await kv.put('stable', 'y', { delay: 0, maxDelay: 0, maxDelayCreate: 0 })
                await kv.flush()
                await kv.close()
                // clear() reopens immediately: stale rewrite counters from the previous session must not trigger a rewrite against the just-deleted file.
                await kv.clear()
                assert(existsSync(FN), 'clear after close recreates storage')
                assert(!readdirSync('.').some(x => x.startsWith(FN + '-delete-me-')), 'clear after close leaves no stale rewrite temp')
                await kv.unlink()
            })
            await measure('clear-after-close-stale-bucket-rewrite-regression', async () => {
                const FN = 'clear-after-close-stale-bucket-rewrite.db'
                const kv = new KvStorage({
                    rewriteLater: false,
                    rewriteOnOpen: true,
                    bucketThreshold: 1,
                    fileThreshold: 1_000_000,
                    memoryThreshold: 1_000_000,
                })
                await kv.open(FN, { clear: true })
                await kv.put('stable', 'first bucket payload', { delay: 0, maxDelay: 0, maxDelayCreate: 0 })
                await kv.flush()
                await kv.put('stable', 'second bucket payload', { delay: 0, maxDelay: 0, maxDelayCreate: 0 })
                await kv.flush()
                await kv.close()
                // Bucket rewrite counters also belong to the previous session and must not survive clear()+open().
                await kv.clear()
                assert(existsSync(FN), 'clear after close recreates storage after bucket writes')
                assert(!existsSync(FN + '-bucket'), 'clear after close does not recreate bucket without entries')
                await kv.unlink()
            })
            await measure('unlink-during-rewrite-regression', async () => {
                const FN = 'unlink-during-rewrite.db'
                const kv = new KvStorage({ rewriteOnOpen: false })
                await kv.open(FN, { clear: true })
                await kv.put('stable', 'first value')
                await kv.put('stable', 'second value')
                await kv.flush()
                let unlinkPromise
                kv.once('rewrite', () => {
                    // Queue unlink exactly while rewrite owns lockWrite, so we verify unlink waits instead of deleting under an active swap.
                    unlinkPromise = kv.unlink()
                })
                await kv.rewrite()
                await unlinkPromise
                assert(!existsSync(FN), 'unlink after rewrite removes main file')
                // replaceFile() removes its old-main temp asynchronously, so wait for the normal cleanup instead of asserting too early.
                await eventually(() => !readdirSync('.').some(x => x.startsWith(FN + '-delete-me-')))
                assert(true, 'unlink after rewrite leaves no rewrite temp')
            })
            await measure('multi-open-regression', async () => {
                const FN = 'lock.db'
                const first = new KvStorage({ rewriteOnOpen: false })
                await first.open(FN, { clear: true })
                const second = new KvStorage({ rewriteOnOpen: false })
                await second.open(FN)
                assert(first.isOpen() && second.isOpen(), 'multiple instances can open same storage')
                await second.put('k', 'v')
                assert(await second.get('k') === 'v', 'second instance can write')
                await second.close()
                await first.close()
                const reloaded = new KvStorage({ rewriteOnOpen: false })
                await reloaded.open(FN)
                assert(await reloaded.get('k') === 'v', 'value persists after reopening')
                await reloaded.unlink()
            })
            await measure('cleanup-on-open-isolation-regression', async () => {
                const base = 'cleanup.db'
                const ownMainTemp = base + '-delete-me-own'
                const ownBucketTemp = base + '-bucket-delete-me-own'
                const siblingTemp = base + '2-delete-me-sibling'
                for (const f of [ownMainTemp, ownBucketTemp, siblingTemp])
                    if (readdirSync('.').includes(f))
                        unlinkSync(f)
                writeFileSync(ownMainTemp, '')
                writeFileSync(ownBucketTemp, '')
                writeFileSync(siblingTemp, '')
                const cleanup = new KvStorage({ rewriteOnOpen: false })
                await cleanup.open(base, { clear: true })
                const files = readdirSync('.')
                assert(!files.includes(ownMainTemp), 'cleanup removes own main temp')
                assert(!files.includes(ownBucketTemp), 'cleanup removes own bucket temp')
                assert(files.includes(siblingTemp), 'cleanup does not remove sibling temp')
                await cleanup.unlink()
                unlinkSync(siblingTemp)
            })
            await measure('rewrite-bucket-repeat-regression', async () => {
                const FN = 'rewrite-bucket-repeat.db'
                const kv = new KvStorage({ rewriteOnOpen: false, bucketThreshold: 1, fileThreshold: 1_000_000 })
                let rewriteBucketCount = 0
                kv.on('rewriteBucket', () => rewriteBucketCount++)
                await kv.open(FN, { clear: true })
                await kv.put('k', 'first bucket payload')
                await kv.flush()
                await kv.rewriteBucket()
                await kv.put('k', 'second bucket payload')
                await kv.flush()
                await kv.rewriteBucket()
                assert(rewriteBucketCount === 2, `rewrite bucket count ${rewriteBucketCount}`)
                await kv.unlink()
            })
            await measure('sublevel-preexisting-keys-regression', async () => {
                const kv = new KvStorage({ rewriteOnOpen: false })
                await kv.open('sublevel-preexisting-keys.db', { clear: true })
                await kv.put('users\talice', 1)
                await kv.put('users\tbob', 2)
                await kv.flush()
                const users = kv.sublevel('users')
                assert(users.has('alice'), 'sublevel has preexisting key')
                const listed = Array.from(users.keys({}))
                assert(listed.includes('alice'), `sublevel keys include stripped key ${JSON.stringify(listed)}`)
                await users.unlink()
                assert(await kv.get('users\talice') === undefined, 'sublevel unlink removes alice')
                assert(await kv.get('users\tbob') === undefined, 'sublevel unlink removes bob')
                await kv.unlink()
            })
            await measure('sublevel-stale-sync-regression', async () => {
                const kv = new KvStorage({ rewriteOnOpen: false })
                await kv.open('sublevel-stale-sync.db', { clear: true })
                const users = kv.sublevel('users')
                await users.put('alice', 1)
                await kv.flush()
                await kv.del('users\talice')
                await kv.flush()
                assert(!users.has('alice'), 'sublevel has follows parent delete')
                assert(Array.from(users.keys({})).length === 0, 'sublevel keys follow parent delete')
                await users.put('bob', undefined)
                await kv.flush()
                assert(!users.has('bob'), 'sublevel put undefined does not keep ghost key')
                await kv.unlink()
            })
            await measure('external-file-replace', async () => {
                const FN2 = 'ext-replace.db'
                const kv = new KvStorage({ fileThreshold: 10, rewriteOnOpen: false })
                await kv.open(FN2, { clear: true })
                const big1 = Buffer.alloc(50, 'A')
                const big2 = Buffer.alloc(50, 'B')
                await kv.put('big', big1)
                await kv.flush()
                const folder = FN2 + '$'
                const filesBefore = readdirSync(folder)
                assert(filesBefore.length === 1, `first external file created: ${filesBefore.length}`)
                await kv.put('big', big2)
                await kv.flush()
                const filesAfter = readdirSync(folder)
                assert(filesAfter.length === 1, `old external file replaced: ${filesAfter.length}`)
                assert(big2.equals(await kv.get('big')), 'external file updated value')
                await kv.unlink()
            })
            await measure('external-file-collision-after-reopen', async () => {
                const FN2 = 'ext-collision-reopen.db'
                let kv = new KvStorage({ fileThreshold: 1 })
                await kv.open(FN2, { clear: true })
                await kv.put('abcdefghij1', Buffer.from('one'))
                await kv.put('abcdefghij2', Buffer.from('two'))
                await kv.flush()
                await kv.close()
                kv = new KvStorage({ fileThreshold: 1 })
                await kv.open(FN2)
                await kv.put('abcdefghij3', Buffer.from('three'))
                await kv.flush()
                assert((await kv.get('abcdefghij2')).toString() === 'two', 'external file collision survives reopen')
                assert((await kv.get('abcdefghij3')).toString() === 'three', 'external file collision writes new suffix')
                await kv.unlink()
            })
            await measure('external-file-default-name-prevents-traversal', async () => {
                const FN2 = 'ext-default-name.db'
                const outside = 'escapeF'
                if (existsSync(outside))
                    unlinkSync(outside)
                const kv = new KvStorage({ fileThreshold: 1 })
                await kv.open(FN2, { clear: true })
                await kv.put('../escapeFile', Buffer.from('safe'))
                await kv.put('a/b.c', Buffer.from('nested'))
                await kv.flush()
                assert(!existsSync(outside), 'default external filename prevents traversal')
                assert(existsSync(join(FN2 + '$', 'a', 'b.c')), 'default external filename keeps subfolders')
                assert((await kv.get('../escapeFile')).toString() === 'safe', 'default external filename remains readable')
                await kv.unlink()
            })
            const lastOften = await new Promise(res => {
                const K = 'often'
                let wrote = 0
                db.on('wrote', ({ key }) => (key === K) && ++wrote) // count
                let insteadOf = 0
                const h = setInterval(() => db.put(K, ++insteadOf, { delay: 200, maxDelay: 1000, maxDelayCreate: 0 }), 100)
                setTimeout(() => {
                    clearInterval(h)
                    assert(wrote === 2, `often ${wrote}/${insteadOf}`)
                    db.flush().then(() => res(insteadOf))
                }, 1800)
            })
            await measure('coalescing', async () => {
                const hot = new KvStorage({ defaultPutDelay: 1000, maxPutDelay: 1000, maxPutDelayCreate: 1000 })
                await hot.open(FN + '-hot', { clear: true })
                for (let i = 0; i < 50; i++)
                    hot.put('same-key', i)
                assert(hot.listenerCount('flush') <= 1, `coalescing failed: ${hot.listenerCount('flush')} flush listeners`)
                await hot.flush()
                assert(await hot.get('same-key') === 49, 'coalesced final value')
                hot.put('same-key', 50)
                await hot.flush()
                assert(await hot.get('same-key') === 50, 'coalesced update after flush')
                assert(!readFileSync(FN + '-hot', 'utf8').includes('"pending"'), 'pending is not persisted')
                await hot.unlink()
            })
            const MUL = 10000
            const BN = MUL / 10
            await measure('write', async () => {
                // these should not be written because overwritten
                for (let i = 1; i <= MUL; i++) db.put('o'+i, { prop: "first" + i })

                for (let i = 1; i <= BN; i++) db.put('b'+i, buf)
                db.put('jb64', { buf }) // this is supposed to end in bucket
                db.put('b0', Buffer.from(bigBuf)) // this should write a separate binary file, because it's a Buffer
                db.del('b0')
                for (let i = 1; i <= MUL; i++) db.put('o'+i, { prop: "second" + i })
                await db.flush()
                // these should trigger rewrite
                for (let i = 1; i <= MUL; i++) db.put('o'+i, { prop: "rewritten" + i })
                db.put('z', 1)
            })
            await measure('flush', () => db.flush()) // first write previous ones
            assert(!db.getSync('b1'), 'bucket offloaded')
            assert(buf.equals(await db.get('b1')), 'get bucket')
            assert(buf.equals(await db.get('b' + BN)), 'get other bucket')
            await measure('put+rewrite', async () => { // these should trigger rewrite
                // time here is both for put-s and the triggered rewrite
                for (let i = 1; i <= MUL; i++) db.put('o' + i, { prop: "rewrittenAgain" + i })
                await db.flush() // first write previous ones
            })
            await measure('rewrite-bucket', async () => { // these should trigger rewrite
                for (let i = 1; i <= BN / 2; i++) db.del('b' + i)
                //db.wouldSave = db.fileSize // trick the lib into causing rewrite of main file as bucket is rewritten
                await db.flush()
            })
            await measure('close', () => db.close())
            await measure('read', () => db.open(FN)) // just a benchmark
            const content = readFileSync(FN, 'utf-8')
            assert(content.includes(`{"k":"often","v":${lastOften}`), 'lastOften')
            assert(!content.includes('"b0"'), 'b0')
            // half buckets were deleted
            assert(content.includes(`{"k":"jb64","bucket":[${BN/2 * buf.length},26705],"format":"json"}`), 'jb64')
            assert(content.includes(`{"k":"k1","v":"v1"}`), 'k1')
            assert(content.includes(`{"k":"b${BN}","bucket"`), 'bN')
            const jbPath = JSON.stringify(join('j', 'jb'))
            assert(content.includes(`{"k":"jb","file":${jbPath},"format":"json"}`), 'jb')
            assert(content.includes(`{"k":"o1","v":{"prop":"rewrittenAgain1"}}`), 'o0')
            assert(content.includes(`{"k":"o${MUL}","v":{"prop":"rewrittenAgain${MUL}"}}`), 'oMUL')
            assert(content.includes('while-rewriting'), 'while-rewriting')
            let extra = 0
            for (const v of db.map.values())
                extra += v.file?.match(/\\/g)?.length || 0 // this path separator uses 1 extra byte once encoded
            const finalSize = statSync(FN).size
            assert(finalSize === 541908 + extra, `final size ${finalSize}`)
            assert(decodeErrors === 0, `errorDecoding ${decodeErrors}`)
            console.log('final size: ', finalSize.toLocaleString())
            await db.close()
        })
    }
    finally {
        //db.unlink()
    }
}

async function measure(label, cb) {
    console.log('START', label)
    const t = Date.now()
    await cb()
    console.log('FINISHED', label, Date.now() - t, 'ms')
}

async function eventually(cb, { timeout=3000, interval=50 }={}) {
    const until = Date.now() + timeout
    while (Date.now() < until) {
        if (cb())
            return
        await new Promise(res => setTimeout(res, interval))
    }
    throw Error('timeout waiting for condition')
}
