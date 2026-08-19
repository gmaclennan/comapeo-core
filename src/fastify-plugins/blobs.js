import fp from 'fastify-plugin'
import { filetypemime } from 'magic-bytes.js'
import { pEvent } from 'p-event'
import { Type as T } from '@sinclair/typebox'

import { SUPPORTED_BLOB_VARIANTS } from '../blob-store/index.js'
import { HEX_REGEX_32_BYTES, Z_BASE_32_REGEX_32_BYTES } from './constants.js'
import { ensureKnownError } from '../errors.js'
import {
  BlobNotFoundError,
  BlobStoreEntryNotFoundError,
  RangeNotSatisfiableError,
  UnsupportedVariantError,
} from '../errors.js'
import ensureError from 'ensure-error'

/** @import { BlobId } from '../types.js' */

export default fp(blobServerPlugin, {
  fastify: '4.x',
  name: 'mapeo-blobs',
})

/**
 * @typedef {Object} BlobServerPluginOpts
 *
 * @property {(projectPublicId: string) => Promise<import('../blob-store/index.js').BlobStore>} getBlobStore
 */

const BLOB_TYPES = /** @type {BlobId['type'][]} */ (
  Object.keys(SUPPORTED_BLOB_VARIANTS)
)
const BLOB_VARIANTS = [
  ...new Set(Object.values(SUPPORTED_BLOB_VARIANTS).flat()),
]

const PARAMS_JSON_SCHEMA = T.Object({
  projectPublicId: T.String({ pattern: Z_BASE_32_REGEX_32_BYTES }),
  driveId: T.String({ pattern: HEX_REGEX_32_BYTES }),
  type: T.Union(
    BLOB_TYPES.map((type) => {
      return T.Literal(type)
    })
  ),
  variant: T.Union(
    BLOB_VARIANTS.map((variant) => {
      return T.Literal(variant)
    })
  ),
  name: T.String(),
})

/** @type {import('fastify').FastifyPluginAsync<import('fastify').RegisterOptions & BlobServerPluginOpts>} */
async function blobServerPlugin(fastify, options) {
  if (!options.getBlobStore) throw new TypeError('Missing getBlobStore')

  // We call register here so that the `prefix` option can work if desired
  // https://fastify.dev/docs/latest/Reference/Routes#route-prefixing-and-fastify-plugin
  fastify.register(routes, options)
}

/** @type {import('fastify').FastifyPluginAsync<Omit<BlobServerPluginOpts, 'prefix'>, import('fastify').RawServerDefault, import('@fastify/type-provider-typebox').TypeBoxTypeProvider>} */
async function routes(fastify, options) {
  const { getBlobStore } = options

  // `Content-Range` is only meaningful on a 206 or a 416. A blob stream can
  // fail after we have set it, so make sure it never ends up on any other
  // response.
  fastify.addHook('onError', async (_request, reply) => {
    if (reply.statusCode !== 416) reply.removeHeader('Content-Range')
  })

  fastify.get(
    '/:projectPublicId/:driveId/:type/:variant/:name',
    { schema: { params: PARAMS_JSON_SCHEMA } },
    async (request, reply) => {
      const { projectPublicId, ...blobId } = request.params

      if (!isValidBlobId(blobId)) {
        reply.code(400)
        throw new UnsupportedVariantError({
          variant: blobId.variant,
          type: blobId.type,
        })
      }
      const { driveId } = blobId

      let blobStore
      try {
        blobStore = await getBlobStore(projectPublicId)
      } catch (e) {
        reply.code(404)
        throw ensureKnownError(e)
      }

      let entry
      try {
        entry = await blobStore.entry(blobId, { wait: false })
      } catch (e) {
        reply.code(404)
        throw ensureKnownError(e)
      }

      if (!entry) {
        reply.code(404)
        throw new BlobStoreEntryNotFoundError()
      }

      const { metadata, blob } = entry.value
      const blobLength = blob.byteLength
      const metadataMimeType = getMetadataMimeType(metadata)

      reply.header('Accept-Ranges', 'bytes')

      const range = parseRange(request.headers.range, blobLength)

      // `null` means the requested range cannot be satisfied
      if (range === null) {
        reply.code(416)
        reply.header('Content-Range', `bytes */${blobLength}`)
        throw new RangeNotSatisfiableError()
      }

      if (range) {
        reply.code(206)
        reply.header(
          'Content-Range',
          `bytes ${range.start}-${range.end}/${blobLength}`
        )
        reply.header('Content-Length', range.end - range.start + 1)
      } else {
        reply.header('Content-Length', blobLength)
      }

      // An empty blob has no blocks to read, so there is no stream to wait on
      if (blobLength === 0) {
        reply.header(
          'Content-Type',
          metadataMimeType || 'application/octet-stream'
        )
        return reply.send('')
      }

      let blobStream
      try {
        blobStream = await blobStore.createReadStreamFromEntry(
          driveId,
          entry,
          range && { start: range.start, length: range.end - range.start + 1 }
        )
      } catch (e) {
        reply.code(404)
        throw ensureKnownError(e)
      }

      try {
        await pEvent(blobStream, 'readable', { rejectionEvents: ['error'] })
      } catch (e) {
        // This matches [how Hyperblobs checks if a blob is unavailable][0].
        // [0]: https://github.com/holepunchto/hyperblobs/blob/518088d2b828082fd70a276fa2c8848a2cf2a56b/index.js#L49
        if (ensureError(e).message === 'Block not available') {
          reply.code(404)
          throw new BlobNotFoundError()
        } else {
          throw ensureKnownError(e)
        }
      }

      if (metadataMimeType) {
        reply.header('Content-Type', metadataMimeType)
      } else {
        // Attempt to guess the MIME type from the bytes at the start of the
        // blob. Those bytes may not have been downloaded, even when the bytes
        // we are serving have been, so fall back rather than failing.
        const blobSlice = await blobStore.getEntryBlob(driveId, entry, {
          length: Math.min(20, blobLength),
        })

        const [guessedMime] = blobSlice ? filetypemime(blobSlice) : []

        reply.header('Content-Type', guessedMime || 'application/octet-stream')
      }

      return reply.send(blobStream)
    }
  )
}

/**
 * @param {unknown} metadata
 * @returns {undefined | string}
 */
function getMetadataMimeType(metadata) {
  if (
    metadata &&
    typeof metadata === 'object' &&
    'mimeType' in metadata &&
    typeof metadata.mimeType === 'string'
  ) {
    return metadata.mimeType
  }
}

// Range units are case-insensitive
const BYTES_RANGE_REGEX = /^bytes=(\d*)-(\d*)$/i

/**
 * Parse a `Range` request header for a single range of bytes.
 *
 * Returns `undefined` if the whole blob should be sent: either no range was
 * requested, or the request is one that [the spec allows a server to
 * ignore][0] — an unknown range unit, a malformed or invalid range, or more
 * than one range (which we don't support). Returns `null` if the requested
 * range cannot be satisfied.
 *
 * [0]: https://www.rfc-editor.org/rfc/rfc9110#field.range
 *
 * @param {undefined | string} rangeHeader
 * @param {number} size Size of the blob, in bytes
 * @returns {undefined | null | { start: number, end: number }} `end` is inclusive
 */
function parseRange(rangeHeader, size) {
  if (!rangeHeader) return undefined

  const match = BYTES_RANGE_REGEX.exec(rangeHeader)
  if (!match) return undefined
  const [, firstPos, lastPos] = match

  let start, end

  if (firstPos === '') {
    // A suffix range, e.g. `bytes=-100`, asks for the last 100 bytes
    if (lastPos === '') return undefined
    const suffixLength = Number(lastPos)
    if (suffixLength === 0) return null
    start = Math.max(0, size - suffixLength)
    end = size - 1
  } else {
    start = Number(firstPos)
    // An absent last position, e.g. `bytes=100-`, asks for the rest of the blob
    const lastBytePos = lastPos === '' ? size - 1 : Number(lastPos)
    // An inverted range, e.g. `bytes=5-2`, is invalid rather than unsatisfiable
    if (lastPos !== '' && lastBytePos < start) return undefined
    end = Math.min(lastBytePos, size - 1)
  }

  return start > end ? null : { start, end }
}

/**
 * @param {Omit<BlobId, 'variant'> & { variant: BlobId['variant'] }} maybeBlobId
 * @returns {maybeBlobId is BlobId}
 */
function isValidBlobId(maybeBlobId) {
  const { type, variant } = maybeBlobId
  /** @type {readonly BlobId['variant'][]} */
  const validVariants = SUPPORTED_BLOB_VARIANTS[type]
  return validVariants.includes(variant)
}
