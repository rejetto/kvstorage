import assert from 'node:assert/strict'
import { KvStorage } from 'index'

test()

async function test() {
    const bytes = [Math.random()*100]
    for (let i = 0; i < 20_000; i++) bytes.push(i % 256)
    const buf = Buffer.from(bytes)

    let db = new KvStorage()
    db.on('rewrite', () => console.log('rewrite'))
    try {
        await measure('basics', async () => {
            await db.open('test.db')
            db.put('k1', 'v1')
            assert(await db.get('k1') === 'v1', "no-await")
            db.put('k2', 2)
            db.put('delete', 3)
            db.delete('delete')
            db.put('b', buf)
            db.put('jb', { buf })
            db.put('k2', 22)
            process.exit(0)
            // reopen to check persistency
            await db.flush()
            db = new KvStorage()
            await db.open('test.db')
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
            db.put('z', true)
            db.put('b0', Buffer.from(buf)) // this should not write
            db.delete('b0')
        })
        console.log('test done')
    }
    finally {
        db.unlink()
    }
}

async function measure(label, cb) {
    console.time(label)
    await cb()
    console.timeEnd(label)
}