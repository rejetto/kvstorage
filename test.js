const { KvStorage } = require('.')
const { statSync } = require('node:fs')
const { readFileSync, readdirSync } = require('fs')
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
    for (let i = 0; i < 20_000; i++) bytes.push(i % 256)
    const buf = Buffer.from(bytes)
    const bigBuf = Buffer.from(bytes.concat(bytes, bytes, bytes, bytes, bytes))
    const FN = 'test.db'
    let db = new KvStorage()
    const original = db.keyToFileName
    db.keyToFileName = k => {
        const x = original(k)
        return join(x[0], x)
    }
    try {
        await measure('all', async () => {
            await measure('basics', async () => {
                await db.open(FN, { clear: true })
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
                assert(db.firstKey() === 'k1', "firstKey")
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
                // reopen to check persistency
                await db.close()
                const expectedSize = 8
                assert(db.size() === expectedSize, "bad size")
                db = new KvStorage({ rewriteLater: true })
                db.on('rewrite', () => {
                    console.log('rewriting to save', db.wouldSave.toLocaleString())
                    db.put('while-rewriting', 1)
                    const t = Date.now()
                    db.lockFlush.then(() => console.log('rewrite finished in', Date.now() - t, 'ms'))
                })
                db.on('rewriteBucket', () =>
                    console.log('rewriting bucket to save', db.bucketWouldSave.toLocaleString()))
                assert(! readdirSync('.').filter(x => x.startsWith(FN + '-win-')).length, "rewrite leftovers") // these would accumulate in time (until process exit)
                await db.open(FN)
                assert(db.size() === expectedSize, "bad size after reload")
                assert(await db.get('k1') === 'v1', "put+get")
                assert(await db.get('k2') === 22, "numbers")
                assert(await db.get('delete') === undefined, "delete")
                assert(db.getSync(`P1${sep}k2 under 1`).getTime() === date.getTime(), "Date+getSync")
                assert(db.map.get('b')?.file, "binary file")
                assert(db.map.get('jb')?.file, "json file")
                assert((await db.get('b'))?.toString('base64') === bigBuf.toString('base64'), "buffer")
                assert((await db.get('jb'))?.bigBuf?.toString('base64') === bigBuf.toString('base64'), "json-buffer")
            })
            const lastOften = await new Promise(res => {
                const K = 'often'
                let wrote = 0
                db.on('wrote', ({ key }) => (key === K) && ++wrote) // count
                let insteadOf = 0
                const h = setInterval(() => db.put(K, ++insteadOf, { delay: 200, maxDelay: 1000, maxDelayCreate: 0 }), 100)
                setTimeout(() => {
                    clearInterval(h)
                    assert(wrote === 3, `often ${wrote}/${insteadOf}`)
                    db.flush().then(() => res(insteadOf))
                }, 1800)
            })
            const MUL = 10000
            const BN = MUL / 10
            await measure('write', async () => {
                // these should not be written because overwritten
                for (let i = 1; i <= MUL; i++) db.put('o'+i, { prop: "first" + i })

                for (let i = 1; i <= BN; i++) db.put('b'+i, buf)
                db.put('jb64', { buf }) // this is supposed to end in bucket
                db.put('b0', Buffer.from(bigBuf)) // this should write because direct buffers are not checked for content
                db.del('b0')
                for (let i = 1; i <= MUL; i++) db.put('o'+i, { prop: "second" + i })
                await db.flush()
                // these should trigger rewrite
                for (let i = 1; i <= MUL; i++) db.put('o'+i, { prop: "rewritten" + i })
                db.put('z', 1)
            })
            await measure('flush', () => db.flush()) // first write previous ones
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
            assert(finalSize === 541896 + extra, `final size ${finalSize}`)
            console.log('final size: ', finalSize.toLocaleString())
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