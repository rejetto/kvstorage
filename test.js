const { KvStorage } = require('.')

test().catch(console.error)

function assert(truth, msg) {
    if (!truth)
        throw "FAILED " + msg
    console.log('OK', msg)
}

async function test() {
    const bytes = [Math.random()*100]
    for (let i = 0; i < 20_000; i++) bytes.push(i % 256)
    const buf = Buffer.from(bytes)
    const FN = 'test.db'
    let db = new KvStorage()
    db.on('rewrite', () => console.log('rewrite'))
    try {
        await measure('basics', async () => {
            await db.open(FN, { clear: true })
            assert(db.size() === 0, "empty")
            db.put('k1', 'v1')
            assert(await db.get('k1') === 'v1', "no-await")
            db.put('k2', 2)
            db.put('delete', 3)
            db.del('delete')
            assert(db.size() === 2, "bad size")
            db.put('b', buf)
            await db.put('jb', { buf })
            db.put('jb64', { buf: db.b64(buf) })
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
            sub1.put('k under 1', 11)
            sub1.put('k2 under 1', 2)
            sub1.put('k2 under 1', 12)
            assert(await db.get(`P1${KvStorage.subSeparator}k under 1`), "sub from above")
            assert(await sub1.get('k under 1'), "sub from inside")
            assert(sub1.size() === 2, "sub size")
            assert(Array.from(sub1.keys()).length === 2, "sub length")
            const sub2 = sub1.sublevel('P2')
            sub2.put('k under 2', 21)
            sub2.put('k2 under 2', 22)
            assert(await db.get(`P1${KvStorage.subSeparator}P2${KvStorage.subSeparator}k under 2`), "sub2 from above")
            assert(sub1.size() === 4, "sub father size")
            assert(sub2.size() === 2, "sub child size")
            assert(await sub2.get('k under 2') === 21, "sub get")
            // reopen to check persistency
            await db.close()
            db = new KvStorage()
            await db.open(FN)
            assert(await db.get('k1') === 'v1', "put+get")
            assert(await db.get('k2') === 22, "numbers")
            assert(await db.get('delete') === undefined, "delete")
            assert(db.map.get('b')?.file, "binary file")
            assert(db.map.get('jb')?.file, "json file")
            assert((await db.get('b'))?.toString('base64') === buf.toString('base64'), "buffer")
            assert((await db.get('jb'))?.buf?.toString('base64') === buf.toString('base64'), "json-buffer")
        })
        await measure('write', async () => {
            const MUL = 100
            for (let i = 0; i < 10 * MUL; i++) db.put('o'+i, { prop: Math.random() })
            for (let i = 0; i < MUL; i++) db.put('b'+i, buf)
            db.put('b0', Buffer.from(buf)) // this should write because direct buffers are not checked for content
            db.del('b0')
            db.put('z', true)
        })
        console.log('test done')
    }
    finally {
        //db.unlink()
    }
}

async function measure(label, cb) {
    console.log('start', label)
    console.time(label)
    await cb()
    console.timeEnd(label)
}