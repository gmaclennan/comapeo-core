// Review reproductions for digidem/comapeo-core#1254 (RemoteDiscovery).
// Each test here encodes the behaviour we expect and is EXPECTED TO FAIL on
// the PR head until the corresponding issue is fixed.

import createTestnet from 'hyperdht/testnet.js'
import test from 'node:test'
import assert from 'node:assert/strict'
import { KeyManager } from '@mapeo/crypto'
import { pEvent, TimeoutError as PEventTimeoutError } from 'p-event'
import Protomux from 'protomux'
import cenc from 'compact-encoding'
import { Duplex } from 'streamx'
import pDefer from 'p-defer'
import {
  RemoteDiscovery,
  kTestOnlyHandleHyperswarmConnection,
  AUTH_PROTOCOL,
} from '../../src/discovery/remote-discovery.js'
import { Hello } from '../../src/generated/auth.js'
import {
  ensureKnownError,
  InvalidIdentityProofError,
} from '../../src/errors.js'

/** @import {OpenedNoiseStream} from '../../src/lib/noise-secret-stream-helpers.js' */
/** @import {Keypair} from '../../src/discovery/local-discovery.js' */

// Mirrors the (unexported) AUTH_HANDSHAKE_TIMEOUT in remote-discovery.js
const AUTH_HANDSHAKE_TIMEOUT = 10_000

/** @param {number} seed */
function keypair(seed) {
  return new KeyManager(Buffer.alloc(16, seed)).getIdentityKeypair()
}

function noop() {}

/**
 * Creates a pair of connected duplex streams that pipe data to each other.
 * @returns {[Duplex, Duplex]}
 */
function createStreamPair() {
  const s1 = new Duplex({
    read() {},
    write(chunk, cb) {
      if (s2 && !s2.destroyed) s2.push(chunk)
      cb()
    },
  })
  const s2 = new Duplex({
    read() {},
    write(chunk, cb) {
      if (s1 && !s1.destroyed) s1.push(chunk)
      cb()
    },
  })
  return [s1, s2]
}

/**
 * Mock the server side noise stream passed to #handleHyperswarmConnection.
 * @param {import('streamx').Duplex} stream
 * @param {Keypair} swarmKeypair
 * @param {Buffer} [handshakeHash]
 * @returns {OpenedNoiseStream}
 */
function makeServerStream(
  stream,
  swarmKeypair,
  handshakeHash = Buffer.alloc(32, 0)
) {
  // @ts-ignore mocking
  stream.remotePublicKey = swarmKeypair.publicKey
  // @ts-ignore mocking
  stream.handshakeHash = handshakeHash
  // @ts-ignore mocking
  stream.opened = Promise.resolve(true)
  // @ts-ignore mocking
  stream.userData = undefined
  return /** @type {OpenedNoiseStream} */ (/** @type {unknown} */ (stream))
}

test('REVIEW: a peer that reflects our own identity proof is not authenticated', async (t) => {
  const identityKeypair = keypair(1)
  const swarmKeypair = keypair(3)

  const discovery = new RemoteDiscovery({
    identityKeypair,
    deriveSwarmIdentityKeypair: () => swarmKeypair,
  })
  t.after(() => discovery.close())

  const [serverStream, peerStream] = createStreamPair()
  const serverSocket = makeServerStream(
    serverStream,
    swarmKeypair,
    Buffer.alloc(32, 7)
  )

  const onError = pEvent(discovery, 'error', { timeout: 5000 })
  const onConnection = pEvent(discovery, 'connection', { timeout: 5000 })
  onError.catch(noop)
  onConnection.catch(noop)

  const serverPromise =
    discovery[kTestOnlyHandleHyperswarmConnection](serverSocket)

  // The "attacker" has no identity key of its own. It waits for the server's
  // IdentityProof and sends the exact same bytes back.
  const peerProtomux = Protomux.from(peerStream)
  const onOpen = pDefer()
  const peerChannel = peerProtomux.createChannel({
    protocol: AUTH_PROTOCOL,
    messages: [
      { encoding: cenc.raw, onmessage: noop },
      {
        encoding: cenc.raw,
        /** @param {Buffer} proof */
        onmessage: (proof) => {
          peerChannel.messages[1].send(proof)
        },
      },
    ],
    onopen: () => onOpen.resolve(),
  })
  peerChannel.open()
  await onOpen.promise
  peerChannel.messages[0].send(
    Buffer.from(Hello.encode({ protocolVersion: 1 }).finish())
  )

  const result = await Promise.race([
    onError.then((e) => ({ type: 'error', error: ensureKnownError(e) })),
    onConnection.then((connection) => ({ type: 'connection', connection })),
  ])
  await serverPromise

  if ('connection' in result) {
    const isOwnKey = result.connection.authenticatedPublicKey.equals(
      identityKeypair.publicKey
    )
    assert.fail(
      `reflected identity proof was accepted; authenticatedPublicKey ${
        isOwnKey ? 'is our OWN identity key' : 'is unexpected'
      }`
    )
  }
  assert.equal(
    result.error.code,
    InvalidIdentityProofError.code,
    'reflected proof should be rejected as an invalid identity proof'
  )
})

test('REVIEW: no automatic reconnection after a connectPeer() socket is ended', async (t) => {
  const testnet = await createTestnet(3)
  t.after(() => testnet.destroy())

  const swarmKeypair1 = keypair(3)
  const remoteDiscovery1 = new RemoteDiscovery({
    identityKeypair: keypair(1),
    deriveSwarmIdentityKeypair: () => swarmKeypair1,
    swarm: { dht: testnet.nodes[0] },
  })
  const remoteDiscovery2 = new RemoteDiscovery({
    identityKeypair: keypair(2),
    deriveSwarmIdentityKeypair: () => keypair(4),
    swarm: { dht: testnet.nodes[1] },
  })
  t.after(() =>
    Promise.all([remoteDiscovery1.close(), remoteDiscovery2.close()])
  )
  remoteDiscovery1.on('error', noop)
  remoteDiscovery2.on('error', noop)
  remoteDiscovery1.on('connection', (c) => c.on('error', noop))
  remoteDiscovery2.on('connection', (c) => c.on('error', noop))

  await Promise.all([remoteDiscovery1.start(), remoteDiscovery2.start()])

  const onFirstConnection = pEvent(remoteDiscovery1, 'connection')
  const outbound = await remoteDiscovery2.connectPeer(
    swarmKeypair1.publicKey.toString('hex')
  )
  const inbound = await onFirstConnection
  assert.equal(outbound.isTrusted, true, 'initiator trusts the peer it dialed')

  const onReconnect = pEvent(remoteDiscovery1, 'connection', {
    timeout: 10_000,
  })
  onReconnect.catch(noop)

  // The joiner ends the socket once the join flow is over (or on deny)
  outbound.end()
  await pEvent(inbound, 'close')

  // Hyperswarm must not re-dial the peer on its own: a re-dialed socket would
  // arrive with isTrusted=false on both sides and be handed to the next join.
  await assert.rejects(
    onReconnect,
    PEventTimeoutError,
    'listener side received an unsolicited second connection'
  )
})

test('REVIEW: swarm keypair rotation does not destroy a caller-supplied DHT', async (t) => {
  const testnet = await createTestnet(2)
  t.after(() => testnet.destroy())

  let calls = 0
  const remoteDiscovery = new RemoteDiscovery({
    identityKeypair: keypair(1),
    // Same thing that happens when getSeedTime() moves to a new day
    deriveSwarmIdentityKeypair: () => (calls++ === 0 ? keypair(3) : keypair(5)),
    swarm: { dht: testnet.nodes[0] },
  })
  t.after(() => remoteDiscovery.close().catch(noop))

  await remoteDiscovery.start()
  await remoteDiscovery.stop()

  await assert.doesNotReject(
    remoteDiscovery.start(),
    'start() with a rotated swarm key should succeed'
  )
  assert.equal(
    testnet.nodes[0].destroyed,
    false,
    'the DHT node we were given must not be destroyed'
  )
})

test('REVIEW: inbound peer that never opens the auth channel is dropped within the handshake timeout', async (t) => {
  const discovery = new RemoteDiscovery({
    identityKeypair: keypair(1),
    deriveSwarmIdentityKeypair: () => keypair(3),
  })
  t.after(() => discovery.close())

  const [serverStream, peerStream] = createStreamPair()
  const serverSocket = makeServerStream(serverStream, keypair(3))
  // Peer speaks protomux but never opens `comapeo/auth`
  Protomux.from(peerStream)

  const onError = pEvent(discovery, 'error', {
    timeout: AUTH_HANDSHAKE_TIMEOUT + 5000,
  })
  const serverPromise =
    discovery[kTestOnlyHandleHyperswarmConnection](serverSocket)

  await assert.doesNotReject(
    onError,
    'handler should give up on a silent peer and emit an error'
  )
  await serverPromise
})
