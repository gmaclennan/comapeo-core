import { randomBytes } from 'node:crypto'
import test from 'node:test'
import assert from 'node:assert/strict'
import { readdirSync } from 'fs'
import { readFile } from 'fs/promises'
import path from 'path'
import fastify from 'fastify'

import BlobServerPlugin from '../../src/fastify-plugins/blobs.js'
import { projectKeyToPublicId } from '../../src/utils.js'
import { createBlobStore } from '../helpers/blob-store.js'
import { waitForCores, replicate } from '../helpers/core-manager.js'
import { NotFoundError } from '../../src/errors.js'

test('Plugin throws error if missing getBlobStore option', async () => {
  const server = fastify()
  await assert.rejects(async () => {
    await server.register(BlobServerPlugin)
  })
})

test('Plugin handles prefix option properly', async (t) => {
  const prefix = '/blobs'
  const { data, server, projectPublicId } = await setup(t, { prefix })

  for (const { blobId } of data) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        prefix,
        projectPublicId,
      }),
    })

    assert.equal(res.statusCode, 200, 'request successful')
  }
})

test('Unsupported blob type and variant params are handled properly', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId } of data) {
    const unsupportedVariantRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
        variant: 'foo',
      }),
    })

    assert.equal(unsupportedVariantRes.statusCode, 400)
    assert.equal(unsupportedVariantRes.json().code, 'FST_ERR_VALIDATION')

    const unsupportedTypeRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
        type: 'foo',
      }),
    })

    assert.equal(unsupportedTypeRes.statusCode, 400)
    assert.equal(unsupportedTypeRes.json().code, 'FST_ERR_VALIDATION')
  }
})

test('Invalid variant-type combination returns error', async (t) => {
  const { server, projectPublicId } = await setup(t)

  const url = buildRouteUrl({
    projectPublicId,
    driveId: Buffer.alloc(32).toString('hex'),
    name: 'foo',
    type: 'video',
    variant: 'thumbnail',
  })

  const response = await server.inject({ method: 'GET', url })

  assert.equal(response.statusCode, 400)
  assert(response.json().message.startsWith('Unsupported variant'))
})

test('Incorrect project public id returns 404', async (t) => {
  const { data, server } = await setup(t)

  const incorrectProjectPublicId = projectKeyToPublicId(randomBytes(32))

  for (const { blobId } of data) {
    const incorrectProjectPublicIdRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId: incorrectProjectPublicId,
      }),
    })

    assert.equal(incorrectProjectPublicIdRes.statusCode, 404)
  }
})

test('Incorrectly formatted project public id returns 400', async (t) => {
  const { data, server } = await setup(t)

  const hexString = randomBytes(32).toString('hex')

  for (const { blobId } of data) {
    const incorrectProjectPublicIdRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId: hexString,
      }),
    })

    assert.equal(incorrectProjectPublicIdRes.statusCode, 400)
  }
})

test('Missing blob name or variant returns 404', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId } of data) {
    const nameMismatchRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
        name: 'foo',
      }),
    })

    assert.equal(nameMismatchRes.statusCode, 404)

    const variantMismatchRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
        variant: 'thumbnail',
      }),
    })

    assert.equal(variantMismatchRes.statusCode, 404)
  }
})

test('GET photo returns correct blob payload', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId, image } of data) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
      }),
    })

    assert.deepEqual(res.rawPayload, image.data, 'should be equal')
  }
})

test('GET photo returns inferred content header if metadata is not found', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId, image } of data) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
      }),
    })

    const expectedContentHeader =
      getImageMimeType(image.ext) || 'application/octet-stream'

    assert.equal(
      res.headers['content-type'],
      expectedContentHeader,
      'should be equal'
    )
  }
})

test('GET photo uses mime type from metadata if found', async (t) => {
  const { data, server, projectPublicId, blobStore } = await setup(t)

  for (const { blobId, image } of data) {
    const imageMimeType = getImageMimeType(image.ext)
    const metadata = imageMimeType ? { mimeType: imageMimeType } : undefined

    const driveId = await blobStore.put(blobId, image.data, {
      metadata: imageMimeType ? { mimeType: imageMimeType } : undefined,
    })

    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
        driveId,
      }),
    })

    const expectedContentHeader = metadata
      ? metadata.mimeType
      : 'application/octet-stream'

    assert.equal(
      res.headers['content-type'],
      expectedContentHeader,
      'should be equal'
    )
  }
})

test('GET photo returns 404 when trying to get non-replicated blob', async (t) => {
  const projectKey = randomBytes(32)

  const {
    data,
    projectPublicId,
    coreManager: cm1,
  } = await setup(t, { projectKey })

  const { blobStore: bs2, coreManager: cm2 } = createBlobStore(t, {
    projectKey,
  })

  const [{ blobId }] = data

  const { destroy } = await replicate(cm1, cm2)

  await waitForCores(cm2, [cm1.getWriterCore('blobIndex').key])

  /** @type {any}*/
  const { core: replicatedCore } = cm2.getCoreByDiscoveryKey(
    Buffer.from(blobId.driveId, 'hex')
  )
  await replicatedCore.update({ wait: true })
  await replicatedCore.download({ end: replicatedCore.length }).done()
  await destroy()

  const server = createServer({ blobStore: bs2, projectKey })

  const res = await server.inject({
    method: 'GET',
    url: buildRouteUrl({ ...blobId, projectPublicId }),
  })

  assert.equal(res.statusCode, 404)
})

test('GET photo returns 404 when trying to get non-existent blob', async (t) => {
  const projectKey = randomBytes(32)

  const { projectPublicId, blobStore } = await setup(t, { projectKey })

  const expected = await readFile(new URL(import.meta.url))

  const blobId = /** @type {const} */ ({
    type: 'photo',
    variant: 'original',
    name: 'test-file',
  })

  const server = createServer({ blobStore, projectKey })

  // Test that the blob does not exist
  {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({
        ...blobId,
        projectPublicId,
        driveId: blobStore.writerDriveId,
      }),
    })

    assert.equal(res.statusCode, 404)
  }

  const driveId = await blobStore.put(blobId, expected)
  await blobStore.clear({ ...blobId, driveId: blobStore.writerDriveId })

  // Test that the entry exists but blob does not. A range starting at 0 and
  // one starting part-way in take different paths through Hyperblobs, so
  // check both alongside the plain request.
  for (const range of [undefined, 'bytes=0-9', 'bytes=10-19']) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId, driveId }),
      headers: range ? { range } : {},
    })

    assert.equal(res.statusCode, 404, `${range} is not found`)
    assert.equal(res.headers['content-range'], undefined)
  }
})

test('GET blob advertises range support and content length', async (t) => {
  const { server, projectPublicId, ...ctx } = await setup(t)
  const { blobId, contents } = await putRangeBlob(ctx.blobStore)

  const res = await server.inject({
    method: 'GET',
    url: buildRouteUrl({ ...blobId, projectPublicId }),
  })

  assert.equal(res.statusCode, 200)
  assert.equal(res.headers['accept-ranges'], 'bytes')
  assert.equal(res.headers['content-length'], String(contents.byteLength))
  assert.equal(res.headers['content-range'], undefined)
})

test('GET blob with a range returns that part of the blob', async (t) => {
  const { server, projectPublicId, ...ctx } = await setup(t)
  const { blobId, contents } = await putRangeBlob(ctx.blobStore)
  const size = contents.byteLength

  /**
   * `start` and `end` are the inclusive byte offsets we expect to be served.
   * @type {Array<{ range: string, start: number, end: number }>}
   */
  const cases = [
    { range: 'bytes=0-9', start: 0, end: 9 },
    { range: 'bytes=10-19', start: 10, end: 19 },
    // A single byte
    { range: 'bytes=5-5', start: 5, end: 5 },
    // The first and last byte of the blob
    { range: 'bytes=0-0', start: 0, end: 0 },
    { range: `bytes=${size - 1}-${size - 1}`, start: size - 1, end: size - 1 },
    // Ranges that straddle a Hyperblobs block boundary
    {
      range: `bytes=${BLOCK_SIZE - 5}-${BLOCK_SIZE + 4}`,
      start: BLOCK_SIZE - 5,
      end: BLOCK_SIZE + 4,
    },
    { range: `bytes=0-${BLOCK_SIZE * 2}`, start: 0, end: BLOCK_SIZE * 2 },
    // A range that runs to the end of the blob
    { range: `bytes=10-${size - 1}`, start: 10, end: size - 1 },
    // An open-ended range asks for everything from `start` onwards
    { range: 'bytes=10-', start: 10, end: size - 1 },
    { range: 'bytes=0-', start: 0, end: size - 1 },
    // A suffix range asks for the final N bytes
    { range: 'bytes=-10', start: size - 10, end: size - 1 },
    // An end beyond the blob is clamped to the last byte
    { range: `bytes=10-${size + 100}`, start: 10, end: size - 1 },
    // A suffix longer than the blob is clamped to the whole blob
    { range: `bytes=-${size + 100}`, start: 0, end: size - 1 },
    // Range units are case-insensitive
    { range: 'BYTES=0-9', start: 0, end: 9 },
  ]

  for (const { range, start, end } of cases) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId }),
      headers: { range },
    })

    assert.equal(res.statusCode, 206, `${range} is a partial response`)
    assert.equal(
      res.headers['content-range'],
      `bytes ${start}-${end}/${size}`,
      `${range} has the expected content range`
    )
    assert.equal(
      res.headers['content-length'],
      String(end - start + 1),
      `${range} has the expected content length`
    )
    assert.deepEqual(
      res.rawPayload,
      contents.subarray(start, end + 1),
      `${range} has the expected payload`
    )
  }
})

test('GET photo with a range infers content type from the whole blob', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId, image } of data) {
    const mimeType = getImageMimeType(image.ext)
    if (!mimeType) continue

    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId }),
      // Deliberately skip the start of the blob, where the magic bytes are
      headers: { range: 'bytes=10-19' },
    })

    assert.equal(res.statusCode, 206)
    assert.equal(res.headers['content-type'], mimeType)
  }
})

test('GET blob with an unsatisfiable range returns 416', async (t) => {
  const { server, projectPublicId, ...ctx } = await setup(t)
  const { blobId, contents } = await putRangeBlob(ctx.blobStore)
  const size = contents.byteLength

  const ranges = [
    // Starts at or beyond the end of the blob
    `bytes=${size}-`,
    `bytes=${size}-${size + 10}`,
    // A zero-length suffix cannot be satisfied
    'bytes=-0',
  ]

  for (const range of ranges) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId }),
      headers: { range },
    })

    assert.equal(res.statusCode, 416, `${range} is not satisfiable`)
    assert.equal(
      res.headers['content-range'],
      `bytes */${size}`,
      `${range} reports the blob size`
    )
    assert.notDeepEqual(res.rawPayload, contents)
  }
})

test('GET blob ignores range headers it does not support', async (t) => {
  const { server, projectPublicId, ...ctx } = await setup(t)
  const { blobId, contents } = await putRangeBlob(ctx.blobStore)
  const size = contents.byteLength

  const ranges = [
    // We only support a single range
    'bytes=0-9, 20-29',
    // Unknown range unit
    'items=0-9',
    // Malformed
    'bytes=abc',
    'bytes=-',
    'bytes',
    '',
    // Inverted ranges are invalid rather than unsatisfiable, whether or not
    // they start beyond the end of the blob
    'bytes=5-2',
    `bytes=${size}-${size - 1}`,
    `bytes=${size + 100}-${size}`,
  ]

  for (const range of ranges) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId }),
      headers: { range },
    })

    assert.equal(res.statusCode, 200, `"${range}" returns the whole blob`)
    assert.equal(res.headers['content-range'], undefined)
    assert.deepEqual(res.rawPayload, contents)
  }
})

test('GET blob serves a range that is downloaded even if the rest is not', async (t) => {
  const { server, projectPublicId, coreManager, ...ctx } = await setup(t)
  const { blobId, contents } = await putRangeBlob(ctx.blobStore)

  // Drop the first block of the blob, as if it had not been synced yet
  const entry = await ctx.blobStore.entry(blobId)
  assert(entry)
  const { blockOffset } = entry.value.blob
  await coreManager
    .getWriterCore('blob')
    .core.clear(blockOffset, blockOffset + 1)

  const partialRes = await server.inject({
    method: 'GET',
    url: buildRouteUrl({ ...blobId, projectPublicId }),
    headers: { range: `bytes=${BLOCK_SIZE}-${BLOCK_SIZE + 9}` },
  })

  assert.equal(partialRes.statusCode, 206, 'downloaded bytes are served')
  assert.deepEqual(
    partialRes.rawPayload,
    contents.subarray(BLOCK_SIZE, BLOCK_SIZE + 10)
  )

  const wholeRes = await server.inject({
    method: 'GET',
    url: buildRouteUrl({ ...blobId, projectPublicId }),
  })

  assert.equal(wholeRes.statusCode, 404, 'the whole blob is still unavailable')
})

test('GET blob handles blobs shorter than the bytes used to infer a mime type', async (t) => {
  const { server, projectPublicId, ...ctx } = await setup(t)

  for (const size of [0, 1, 19]) {
    const contents = Buffer.alloc(size, 7)
    const blobId = /** @type {const} */ ({
      type: 'photo',
      variant: 'original',
      name: `short-${size}`,
    })
    const driveId = await ctx.blobStore.put(blobId, contents)
    const url = buildRouteUrl({ ...blobId, projectPublicId, driveId })

    const res = await server.inject({ method: 'GET', url })

    assert.equal(res.statusCode, 200, `${size} byte blob is served`)
    assert.equal(res.headers['content-length'], String(size))
    assert.equal(res.headers['content-type'], 'application/octet-stream')
    assert.deepEqual(res.rawPayload, contents)

    const rangeRes = await server.inject({
      method: 'GET',
      url,
      headers: { range: 'bytes=0-0' },
    })

    // There is no byte 0 to serve from an empty blob
    if (size === 0) {
      assert.equal(rangeRes.statusCode, 416, 'empty blob has no range to serve')
      assert.equal(rangeRes.headers['content-range'], 'bytes */0')
    } else {
      assert.equal(rangeRes.statusCode, 206, `${size} byte blob serves a range`)
      assert.equal(rangeRes.headers['content-range'], `bytes 0-0/${size}`)
      assert.deepEqual(rangeRes.rawPayload, contents.subarray(0, 1))
    }
  }
})

/**
 * @param {object} opts
 * @param {string} [opts.prefix]
 * @param {import('../../src/blob-store/index.js').BlobStore} opts.blobStore
 * @param {Buffer} opts.projectKey
 */
function createServer(opts) {
  return fastify().register(BlobServerPlugin, {
    prefix: opts.prefix,
    getBlobStore: async (projectPublicId) => {
      if (projectPublicId !== projectKeyToPublicId(opts.projectKey)) {
        throw new NotFoundError(
          `Could not get blobStore for project id ${projectPublicId}`
        )
      }
      return opts.blobStore
    },
  })
}

/**
 * @param {import('node:test').TestContext} t
 * @param {object} [opts]
 * @param {string} [opts.prefix]
 * @param {Buffer} [opts.projectKey]
 */
async function setup(t, { prefix, projectKey = randomBytes(32) } = {}) {
  const { blobStore, coreManager } = createBlobStore(t, { projectKey })
  const data = await populateStore(blobStore)

  const server = createServer({ prefix, blobStore, projectKey })

  const projectPublicId = projectKeyToPublicId(projectKey)

  return { data, server, projectPublicId, coreManager, blobStore }
}

/** The block size Hyperblobs splits blobs into */
const BLOCK_SIZE = 64 * 1024

/**
 * Store a blob with known, self-describing contents that spans several
 * Hyperblobs blocks, so that range tests do not depend on fixture sizes.
 *
 * @param {import('../../src/blob-store/index.js').BlobStore} blobStore
 */
async function putRangeBlob(blobStore) {
  const contents = Buffer.from(
    Uint8Array.from({ length: BLOCK_SIZE * 3 + 100 }, (_, i) => i % 251)
  )
  const blobIdBase = /** @type {const} */ ({
    type: 'photo',
    variant: 'original',
    name: 'range-blob',
  })
  const driveId = await blobStore.put(blobIdBase, contents)
  return { blobId: { ...blobIdBase, driveId }, contents }
}

const IMAGE_FIXTURES_PATH = new URL('../fixtures/images', import.meta.url)
  .pathname

const IMAGE_FIXTURES = readdirSync(IMAGE_FIXTURES_PATH)

/**
 * @param {import('../../src/blob-store/index.js').BlobStore} blobStore
 */
async function populateStore(blobStore) {
  /** @type {{blobId: import('../../src/types.js').BlobId, image: {data: Buffer, ext: string}}[]} */
  const data = []

  for (const fixture of IMAGE_FIXTURES) {
    const imagePath = path.resolve(IMAGE_FIXTURES_PATH, fixture)
    const parsedFixture = path.parse(fixture)
    const diskBuffer = await readFile(imagePath)

    const blobIdBase = /** @type {const} */ ({
      type: 'photo',
      variant: 'original',
      name: parsedFixture.name,
    })

    const driveId = await blobStore.put(blobIdBase, diskBuffer)

    data.push({
      blobId: { ...blobIdBase, driveId },
      image: { data: diskBuffer, ext: parsedFixture.ext },
    })
  }

  return data
}

/**
 * @param {string} extension
 * @returns {null | string}
 */
function getImageMimeType(extension) {
  if (extension.startsWith('.')) extension = extension.substring(1)

  if (!['png', 'jpg', 'jpeg'].includes(extension)) {
    return null
  }

  return `image/${extension === 'jpg' ? 'jpeg' : extension}`
}

/**
 *
 * @param {object} opts
 * @param {string} [opts.prefix]
 * @param {string} opts.projectPublicId
 * @param {string} opts.driveId
 * @param {string} opts.type
 * @param {string} opts.variant
 * @param {string} opts.name
 *
 * @returns {string}
 */
function buildRouteUrl({
  prefix = '',
  projectPublicId,
  driveId,
  type,
  variant,
  name,
}) {
  return `${prefix}/${projectPublicId}/${driveId}/${type}/${variant}/${name}`
}
