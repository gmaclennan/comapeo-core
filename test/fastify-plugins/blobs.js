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

  // Test that the entry exists but blob does not
  {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId, driveId }),
    })

    assert.equal(res.statusCode, 404)
  }
})

test('GET photo advertises range support and content length', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId, image } of data) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId }),
    })

    assert.equal(res.statusCode, 200)
    assert.equal(res.headers['accept-ranges'], 'bytes')
    assert.equal(res.headers['content-length'], String(image.data.byteLength))
  }
})

test('GET photo with a range returns that part of the blob', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId, image } of data) {
    const size = image.data.byteLength

    /**
     * `start` and `end` are the inclusive byte offsets we expect to be served.
     * @type {Array<{ range: string, start: number, end: number }>}
     */
    const cases = [
      { range: 'bytes=0-9', start: 0, end: 9 },
      { range: 'bytes=10-19', start: 10, end: 19 },
      // A range that runs to the end of the blob
      { range: `bytes=10-${size - 1}`, start: 10, end: size - 1 },
      // An open-ended range asks for everything from `start` onwards
      { range: 'bytes=10-', start: 10, end: size - 1 },
      { range: 'bytes=0-', start: 0, end: size - 1 },
      // A suffix range asks for the final N bytes
      { range: 'bytes=-10', start: size - 10, end: size - 1 },
      // A single byte
      { range: 'bytes=5-5', start: 5, end: 5 },
      // An end beyond the blob is clamped to the last byte
      { range: `bytes=10-${size + 100}`, start: 10, end: size - 1 },
      // A suffix longer than the blob is clamped to the whole blob
      { range: `bytes=-${size + 100}`, start: 0, end: size - 1 },
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
        image.data.subarray(start, end + 1),
        `${range} has the expected payload`
      )
    }
  }
})

test('GET photo with a range keeps the content type of the whole blob', async (t) => {
  const { data, server, projectPublicId, blobStore } = await setup(t)

  for (const { blobId, image } of data) {
    const inferredRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId }),
      // Deliberately skip the start of the blob, where the magic bytes are
      headers: { range: 'bytes=10-19' },
    })

    assert.equal(
      inferredRes.headers['content-type'],
      getImageMimeType(image.ext) || 'application/octet-stream'
    )

    const driveId = await blobStore.put(blobId, image.data, {
      metadata: { mimeType: 'image/fake' },
    })

    const metadataRes = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId, driveId }),
      headers: { range: 'bytes=10-19' },
    })

    assert.equal(metadataRes.headers['content-type'], 'image/fake')
  }
})

test('GET photo with an unsatisfiable range returns 416', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId, image } of data) {
    const size = image.data.byteLength

    const ranges = [
      // Starts at or beyond the end of the blob
      `bytes=${size}-`,
      `bytes=${size}-${size + 10}`,
      `bytes=${size + 100}-${size + 200}`,
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
    }
  }
})

test('GET photo ignores range headers it does not support', async (t) => {
  const { data, server, projectPublicId } = await setup(t)

  for (const { blobId, image } of data) {
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
      // Inverted, so invalid rather than unsatisfiable
      'bytes=5-2',
    ]

    for (const range of ranges) {
      const res = await server.inject({
        method: 'GET',
        url: buildRouteUrl({ ...blobId, projectPublicId }),
        headers: { range },
      })

      assert.equal(res.statusCode, 200, `"${range}" returns the whole blob`)
      assert.equal(res.headers['content-range'], undefined)
      assert.deepEqual(res.rawPayload, image.data)
    }
  }
})

test('GET photo with a range returns 404 for a missing blob', async (t) => {
  const projectKey = randomBytes(32)
  const { projectPublicId, blobStore } = await setup(t, { projectKey })

  const blobId = /** @type {const} */ ({
    type: 'photo',
    variant: 'original',
    name: 'test-file',
  })

  const driveId = await blobStore.put(
    blobId,
    await readFile(new URL(import.meta.url))
  )
  await blobStore.clear({ ...blobId, driveId })

  const server = createServer({ blobStore, projectKey })

  // A range starting at 0 and one starting part-way through the blob take
  // different paths through Hyperblobs, so check both
  for (const range of ['bytes=0-9', 'bytes=10-19']) {
    const res = await server.inject({
      method: 'GET',
      url: buildRouteUrl({ ...blobId, projectPublicId, driveId }),
      headers: { range },
    })

    assert.equal(res.statusCode, 404, `${range} is not found`)
    assert.equal(res.headers['content-range'], undefined)
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
